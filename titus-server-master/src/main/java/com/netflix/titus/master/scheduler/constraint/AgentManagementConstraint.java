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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerAttributes;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_TOLERATIONS;
import static com.netflix.titus.common.util.StringExt.nonNull;
import static com.netflix.titus.master.scheduler.SchedulerAttributes.TAINTS;
import static com.netflix.titus.master.scheduler.SchedulerUtils.getTier;

/**
 * A system constraint that integrates with agent management in order to determine whether or a not a task
 * should be placed.
 */
@Singleton
public class AgentManagementConstraint implements SystemConstraint {

    public static final String NAME = "AgentManagementConstraint";

    private static final Result VALID = new Result(true, null);

    private enum Failure {
        INSTANCE_GROUP_NOT_FOUND("Instance group not found"),
        INSTANCE_GROUP_NOT_ACTIVE("Instance group is not active or phased out"),
        INSTANCE_GROUP_TIER_MISMATCH("Task cannot run on instance group tier"),
        INSTANCE_GROUP_DOES_NOT_HAVE_GPUS("Instance group does not have gpus"),
        INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS("Instance group does not run non gpu tasks"),

        INSTANCE_NOT_FOUND("Instance not found"),
        INSTANCE_NOT_STARTED("Instance not in Started state"),
        INSTANCE_UNHEALTHY("Unhealthy agent"),

        SYSTEM_NO_PLACEMENT("Cannot place on instance group or agent instance due to systemNoPlacement attribute"),
        NO_PLACEMENT("Cannot place on instance group or agent instance due to noPlacement attribute"),
        TOLERATION_DOES_NOT_MATCH_TAINT("Cannot place on instance group or agent instance due to toleration attribute not matching taint attribute");

        private Result result;

        Failure(String reason) {
            this.result = new Result(false, reason);
        }

        public Result toResult() {
            return result;
        }
    }

    private static final Set<String> FAILURE_REASONS = Stream.of(Failure.values())
            .map(f -> f.toResult().getFailureReason())
            .collect(Collectors.toSet());

    public static boolean isAgentManagementConstraintReason(String reason) {
        return reason != null && FAILURE_REASONS.contains(reason);
    }

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
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, schedulerConfiguration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return Failure.INSTANCE_NOT_FOUND.toResult();
        }

        AgentInstance instance = instanceOpt.get();
        String instanceGroupId = instance.getInstanceGroupId();

        Optional<AgentInstanceGroup> instanceGroupOpt = agentManagementService.findInstanceGroup(instanceGroupId);
        if (!instanceGroupOpt.isPresent()) {
            return Failure.INSTANCE_GROUP_NOT_FOUND.toResult();
        }

        AgentInstanceGroup instanceGroup = instanceGroupOpt.get();
        Result instanceGroupEvaluationResult = evaluateInstanceGroup(instanceGroup);
        if (instanceGroupEvaluationResult != VALID) {
            return instanceGroupEvaluationResult;
        }

        Result instanceEvaluationResult = evaluateInstance(instance);
        if (instanceEvaluationResult != VALID) {
            return instanceEvaluationResult;
        }

        return evaluateTask(v3QueueableTask, instanceGroup, instance);
    }

    private Result evaluateInstanceGroup(AgentInstanceGroup instanceGroup) {
        InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();

        if (state != InstanceGroupLifecycleState.Active && state != InstanceGroupLifecycleState.PhasedOut) {
            return Failure.INSTANCE_GROUP_NOT_ACTIVE.toResult();
        }

        return evaluateInstanceGroupAttributes(instanceGroup);
    }

    private Result evaluateInstance(AgentInstance instance) {
        InstanceLifecycleState state = instance.getLifecycleStatus().getState();
        if (state != InstanceLifecycleState.Started) {
            return Failure.INSTANCE_NOT_STARTED.toResult();
        }

        Result instanceAttributesResult = evaluateAgentInstanceAttributes(instance);
        if (instanceAttributesResult != VALID) {
            return instanceAttributesResult;
        }

        if (!agentStatusMonitor.isHealthy(instance.getId())) {
            return Failure.INSTANCE_UNHEALTHY.toResult();
        }

        return VALID;
    }

    private Result evaluateTask(V3QueueableTask taskRequest, AgentInstanceGroup instanceGroup, AgentInstance instance) {
        Tier tier = getTier(taskRequest);
        if (instanceGroup.getTier() != tier) {
            return Failure.INSTANCE_GROUP_TIER_MISMATCH.toResult();
        }

        boolean gpuTask = isGpuTask(taskRequest);
        boolean gpuAgent = instanceGroup.getResourceDimension().getGpu() > 0;
        if (gpuTask && !gpuAgent) {
            return Failure.INSTANCE_GROUP_DOES_NOT_HAVE_GPUS.toResult();
        }
        if (!gpuTask && gpuAgent) {
            return Failure.INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS.toResult();
        }

        if (!taskCanTolerateTaints(taskRequest, instanceGroup, instance)) {
            return Failure.TOLERATION_DOES_NOT_MATCH_TAINT.toResult();
        }

        return VALID;
    }

    private boolean isGpuTask(V3QueueableTask taskRequest) {
        return taskRequest.getJob().getJobDescriptor().getContainer().getContainerResources().getGpu() > 0;
    }

    private Result evaluateInstanceGroupAttributes(AgentInstanceGroup instanceGroup) {
        return evaluateSchedulingAttributes(instanceGroup.getAttributes());
    }

    private Result evaluateAgentInstanceAttributes(AgentInstance agentInstance) {
        return evaluateSchedulingAttributes(agentInstance.getAttributes());
    }

    private Result evaluateSchedulingAttributes(Map<String, String> attributes) {
        boolean systemNoPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.SYSTEM_NO_PLACEMENT));
        if (systemNoPlacement) {
            return Failure.SYSTEM_NO_PLACEMENT.toResult();
        }

        boolean noPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.NO_PLACEMENT));
        if (noPlacement) {
            return Failure.NO_PLACEMENT.toResult();
        }

        return VALID;
    }

    private boolean taskCanTolerateTaints(V3QueueableTask taskRequest, AgentInstanceGroup instanceGroup, AgentInstance instance) {
        Set<String> taints = getTaints(instanceGroup, instance);
        Set<String> tolerations = getTolerations(taskRequest);
        return tolerations.containsAll(taints);
    }

    private Set<String> getTolerations(V3QueueableTask taskRequest) {
        String jobTolerationValue = nonNull(
                (String) taskRequest.getJob().getJobDescriptor().getAttributes().get(JOB_PARAMETER_ATTRIBUTES_TOLERATIONS)
        ).toLowerCase();
        return StringExt.splitByCommaIntoSet(jobTolerationValue);
    }

    private Set<String> getTaints(AgentInstanceGroup instanceGroup, AgentInstance instance) {
        String instanceGroupTaintsValue = nonNull(instanceGroup.getAttributes().get(TAINTS)).toLowerCase();
        Set<String> instanceGroupTaints = StringExt.splitByCommaIntoSet(instanceGroupTaintsValue);

        String instanceTaintsValue = nonNull(instance.getAttributes().get(TAINTS)).toLowerCase();
        Set<String> instanceTaints = StringExt.splitByCommaIntoSet(instanceTaintsValue);

        return CollectionsExt.merge(instanceGroupTaints, instanceTaints);
    }
}