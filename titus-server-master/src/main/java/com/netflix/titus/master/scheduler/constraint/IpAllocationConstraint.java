/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.master.scheduler.constraint;

import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID;

/**
 * Experimental constraint that matches a machine that can allocate a specific IP.
 */
@Experimental(deadline = "08/2019")
@Singleton
public class IpAllocationConstraint implements SystemConstraint {

    public static final String NAME = "IpAllocationConstraint";
    private static final String IP_ALLOCATION_ALREADY_IN_USE_REASON_PREFIX = "Assigned IP allocation used by another task:";


    private static final Result VALID = new Result(true, null);
    private static final Result MACHINE_DOES_NOT_EXIST = new Result(false, "The machine does not exist");
    private static final Result IP_ALLOCATION_NOT_IN_ZONE = new Result(false, "Assigned IP allocation not in instance's zone");
    private static final Result INVALID_IP_ALLOCATION_ZONE = new Result(false, "Invalid zone for IP allocation");

    private final SchedulerConfiguration configuration;
    private final TaskCache taskCache;
    private final AgentManagementService agentManagementService;

    @Inject
    public IpAllocationConstraint(SchedulerConfiguration configuration,
                                  TaskCache taskCache,
                                  AgentManagementService agentManagementService) {
        this.configuration = configuration;
        this.taskCache = taskCache;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, configuration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return MACHINE_DOES_NOT_EXIST;
        }
        AgentInstance agentInstance = instanceOpt.get();

        Map<String, String> taskContext = ((V3QueueableTask)taskRequest).getTask().getTaskContext();
        if (!taskContext.containsKey(TASK_ATTRIBUTES_IP_ALLOCATION_ID)) {
            // Task has no assigned IP, so any instance will do
            return VALID;
        }
        String ipAllocationId = taskContext.get(TASK_ATTRIBUTES_IP_ALLOCATION_ID);

        // Check if the task's assigned IP allocation is free
        Optional<String> existingTaskAssignedIpAllocationId = taskCache.getTaskByIpAllocationId(ipAllocationId);
        if (existingTaskAssignedIpAllocationId.isPresent()) {
            return getIpAllocationInUseResult(existingTaskAssignedIpAllocationId.get());
        }

        // Find the assigned IP allocation's zone ID
        String instanceZoneId = agentInstance.getAttributes().getOrDefault(configuration.getAvailabilityZoneAttributeName(), "");
        return taskCache.getZoneIdByIpAllocationId(ipAllocationId)
                .map(ipZoneId -> {
                    if (ipZoneId.equals(instanceZoneId)) {
                        return VALID;
                    }
                    return IP_ALLOCATION_NOT_IN_ZONE;
                })
                .orElse(INVALID_IP_ALLOCATION_ZONE);
    }

    public static boolean isInUseIpAllocationConstraintReason(String reason) {
        return reason != null && reason.startsWith(IP_ALLOCATION_ALREADY_IN_USE_REASON_PREFIX);
    }

    public static Optional<String> getTaskIdFromIpAllocationInUseReason(String reason) {
        return reason.startsWith(IP_ALLOCATION_ALREADY_IN_USE_REASON_PREFIX)
                ? Optional.of(reason.substring(IP_ALLOCATION_ALREADY_IN_USE_REASON_PREFIX.length()))
                : Optional.empty();
    }

    private static Result getIpAllocationInUseResult(String taskId) {
        return new Result(false, String.format("%s%s", IP_ALLOCATION_ALREADY_IN_USE_REASON_PREFIX, taskId));
    }
}
