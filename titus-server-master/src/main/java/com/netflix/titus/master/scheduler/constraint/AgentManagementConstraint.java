/*
 * Copyright 2018 Netflix, Inc.
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
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getAttributeValueOrEmptyString;
import static com.netflix.titus.master.scheduler.SchedulerUtils.getTier;

/**
 * A system constraint that integrates with agent management in order to determine whether or a not a task
 * should be placed.
 */
@Singleton
public class AgentManagementConstraint implements SystemConstraint {

    private static final Result MISSING_INSTANCE_GROUP_ATTRIBUTE = new Result(false, "Missing instance group attribute");
    private static final Result INSTANCE_GROUP_NOT_FOUND = new Result(false, "Instance group not found");
    private static final Result INSTANCE_GROUP_NOT_ACTIVE = new Result(false, "Instance group is not active or phased out");
    private static final Result INSTANCE_GROUP_TIER_MISMATCH = new Result(false, "Task cannot run on instance group tier");
    private static final Result INSTANCE_GROUP_DOES_NOT_HAVE_GPUS = new Result(false, "Instance group does not have gpus");
    private static final Result INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS = new Result(false, "Instance group does not run non gpu tasks");

    private static final Result MISSING_INSTANCE_ATTRIBUTE = new Result(false, "Missing instance attribute");
    private static final Result INSTANCE_NOT_FOUND = new Result(false, "Instance not found");
    private static final Result INSTANCE_NOT_STARTED = new Result(false, "Instance not in Started state");
    private static final Result INSTANCE_STATE_IS_OVERRIDDEN = new Result(false, "Instance state is overridden");
    private static final Result INSTANCE_UNHEALTHY = new Result(false, "Unhealthy agent");

    private static final Result TRUE_RESULT = new Result(true, null);

    private static final Set<String> FAILURE_REASONS = CollectionsExt.asSet(
            INSTANCE_GROUP_NOT_FOUND.getFailureReason(),
            MISSING_INSTANCE_GROUP_ATTRIBUTE.getFailureReason(),
            INSTANCE_GROUP_NOT_ACTIVE.getFailureReason(),
            INSTANCE_GROUP_TIER_MISMATCH.getFailureReason(),
            INSTANCE_GROUP_DOES_NOT_HAVE_GPUS.getFailureReason(),
            INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS.getFailureReason(),
            MISSING_INSTANCE_ATTRIBUTE.getFailureReason(),
            INSTANCE_NOT_FOUND.getFailureReason(),
            INSTANCE_NOT_STARTED.getFailureReason(),
            INSTANCE_STATE_IS_OVERRIDDEN.getFailureReason(),
            INSTANCE_UNHEALTHY.getFailureReason()
    );

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;

    @Inject
    public AgentManagementConstraint(SchedulerConfiguration schedulerConfiguration,
                                     AgentManagementService agentManagementService,
                                     AgentStatusMonitor agentStatusMonitor) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
    }

    @Override
    public String getName() {
        return "GlobalAgentClusterConstraint";
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Result instanceGroupEvaluationResult = evaluateInstanceGroup(taskRequest, targetVM);
        if (instanceGroupEvaluationResult != TRUE_RESULT) {
            return instanceGroupEvaluationResult;
        }

        Result InstanceEvaluationResult = evaluateInstance(targetVM);
        if (InstanceEvaluationResult != TRUE_RESULT) {
            return InstanceEvaluationResult;
        }

        return TRUE_RESULT;
    }

    public static boolean isAgentManagementConstraintReason(String reason) {
        return reason != null && FAILURE_REASONS.contains(reason);
    }

    private Result evaluateInstanceGroup(TaskRequest taskRequest, VirtualMachineCurrentState targetVM) {
        String instanceGroupAttributeName = schedulerConfiguration.getInstanceGroupAttributeName();
        String instanceGroupId = getAttributeValueOrEmptyString(targetVM, instanceGroupAttributeName);
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return MISSING_INSTANCE_GROUP_ATTRIBUTE;
        }

        Optional<AgentInstanceGroup> instanceGroupOpt = agentManagementService.findInstanceGroup(instanceGroupId);
        if (!instanceGroupOpt.isPresent()) {
            return INSTANCE_GROUP_NOT_FOUND;
        }

        AgentInstanceGroup instanceGroup = instanceGroupOpt.get();
        InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();

        //TODO safer way to know what is active?
        if (state != InstanceGroupLifecycleState.Active && state != InstanceGroupLifecycleState.PhasedOut) {
            return INSTANCE_GROUP_NOT_ACTIVE;
        }

        Tier tier = getTier((QueuableTask) taskRequest);
        if (instanceGroup.getTier() != tier) {
            return INSTANCE_GROUP_TIER_MISMATCH;
        }

        //TODO read job resource dimensions when we get rid of v2
        boolean gpuTask = taskRequestsGpu(taskRequest);
        boolean gpuAgent = instanceGroup.getResourceDimension().getGpu() > 0;
        if (gpuTask && !gpuAgent) {
            return INSTANCE_GROUP_DOES_NOT_HAVE_GPUS;
        }
        if (!gpuTask && gpuAgent) {
            return INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS;
        }

        return TRUE_RESULT;
    }

    private Result evaluateInstance(VirtualMachineCurrentState targetVM) {
        String instanceId = getAttributeValueOrEmptyString(targetVM, schedulerConfiguration.getInstanceAttributeName());
        if (Strings.isNullOrEmpty(instanceId)) {
            return MISSING_INSTANCE_ATTRIBUTE;
        }

        Optional<AgentInstance> instanceOpt = agentManagementService.findAgentInstance(instanceId);
        if (!instanceOpt.isPresent()) {
            return INSTANCE_NOT_FOUND;
        }

        AgentInstance instance = instanceOpt.get();
        InstanceLifecycleState state = instance.getLifecycleStatus().getState();
        if (state != InstanceLifecycleState.Started) {
            return INSTANCE_NOT_STARTED;
        }

        InstanceOverrideState overrideState = instance.getOverrideStatus().getState();
        if (overrideState != InstanceOverrideState.None) {
            return INSTANCE_STATE_IS_OVERRIDDEN;
        }

        if (!agentStatusMonitor.isHealthy(instanceId)) {
            return INSTANCE_UNHEALTHY;
        }

        return TRUE_RESULT;
    }

    private boolean taskRequestsGpu(TaskRequest taskRequest) {
        Map<String, Double> scalars = taskRequest.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            final Double gpu = scalars.get("gpu");
            return gpu != null && gpu >= 1.0;
        }
        return false;
    }
}