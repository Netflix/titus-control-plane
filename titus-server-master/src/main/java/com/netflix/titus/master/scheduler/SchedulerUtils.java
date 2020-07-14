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

package com.netflix.titus.master.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import org.apache.mesos.Protos;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_TOLERATIONS;
import static com.netflix.titus.common.util.StringExt.nonNull;
import static com.netflix.titus.master.scheduler.SchedulerAttributes.TAINTS;

public class SchedulerUtils {

    public static Tier getTier(QueuableTask queuableTask) {
        Tier tier = Tier.Flex;
        if (queuableTask.getQAttributes().getTierNumber() == 0) {
            tier = Tier.Critical;
        }
        return tier;
    }

    public static boolean hasExclusiveHostHardConstraint(TaskRequest task) {
        return task.getHardConstraints().stream().anyMatch(constraint -> constraint instanceof ExclusiveHostConstraint);
    }

    public static boolean hasGpuRequest(QueuableTask task) {
        return task != null && task.getScalarRequests() != null &&
                task.getScalarRequests().get("gpu") != null &&
                task.getScalarRequests().get("gpu") >= 1.0;
    }

    public static String getAttributeValueOrDefault(Map<String, Protos.Attribute> attributeMap, String attributeName, String defaultValue) {
        if (attributeMap == null) {
            return defaultValue;
        }
        Protos.Attribute attr = attributeMap.get(attributeName);
        if (attr == null || attr.getText() == null) {
            return defaultValue;
        }
        String attrValue = StringExt.safeTrim(attr.getText().getValue());
        return attrValue.isEmpty() ? defaultValue : attrValue;
    }

    public static String getAttributeValueOrDefault(VirtualMachineLease lease, String attributeName, String defaultValue) {
        return getAttributeValueOrDefault(lease.getAttributeMap(), attributeName, defaultValue);
    }

    public static String getAttributeValueOrEmptyString(Map<String, Protos.Attribute> attributeMap, String attributeName) {
        return getAttributeValueOrDefault(attributeMap, attributeName, "");
    }

    public static String getAttributeValueOrEmptyString(VirtualMachineCurrentState targetVM, String attributeName) {
        return getAttributeValueOrEmptyString(targetVM.getCurrAvailableResources().getAttributeMap(), attributeName);
    }

    public static Optional<String> getInstanceGroupName(String instanceGroupAttributeName, VirtualMachineLease lease) {
        String name = getAttributeValueOrEmptyString(lease.getAttributeMap(), instanceGroupAttributeName);
        return name.isEmpty() ? Optional.empty() : Optional.of(name);
    }

    public static Optional<AgentInstance> findInstance(AgentManagementService agentManagementService,
                                                       String instanceIdAttributeName,
                                                       VirtualMachineCurrentState virtualMachineCurrentState) {
        String instanceId = getAttributeValueOrEmptyString(virtualMachineCurrentState, instanceIdAttributeName);
        if (!instanceId.isEmpty()) {
            return agentManagementService.findAgentInstance(instanceId);
        }
        return Optional.empty();
    }

    public static Map<String, Integer> groupCurrentlyAssignedTasksByZoneId(String jobId, Collection<TaskTracker.ActiveTask> tasksCurrentlyAssigned, String zoneAttributeName) {
        Map<String, Integer> result = new HashMap<>();

        for (TaskTracker.ActiveTask activeTask : tasksCurrentlyAssigned) {
            TaskRequest request = activeTask.getTaskRequest();
            if (request instanceof V3QueueableTask) {
                String requestJobId = ((V3QueueableTask) request).getJob().getId();
                if (jobId.equals(requestJobId)) {
                    String zoneId = getAttributeValueOrEmptyString(activeTask.getTotalLease().getAttributeMap(), zoneAttributeName);
                    if (!zoneId.isEmpty()) {
                        result.put(zoneId, result.getOrDefault(zoneId, 0) + 1);
                    }
                }
            }
        }

        return result;
    }

    static Set<V3QueueableTask> collectFailedTasksIgnoring(
            Map<TaskPlacementFailure.FailureKind, Map<V3QueueableTask, List<TaskPlacementFailure>>> failuresByKind,
            Set<TaskPlacementFailure.FailureKind> ignoredFailureKinds) {
        Set<V3QueueableTask> failedTasks = new HashSet<>();
        for (Map.Entry<TaskPlacementFailure.FailureKind, Map<V3QueueableTask, List<TaskPlacementFailure>>> entry : failuresByKind.entrySet()) {
            if (ignoredFailureKinds.contains(entry.getKey())) {
                continue;
            }
            failedTasks.addAll(entry.getValue().keySet());
        }
        return failedTasks;
    }

    public static boolean isLaunchingLessThanNumberOfTasks(Map<String, Task> tasksById, VirtualMachineCurrentState targetVM,
                                                           int numberOfTasks) {
        int launchingTaskCount = 0;
        for (TaskRequest running : targetVM.getRunningTasks()) {
            if (isTaskLaunching(tasksById, running)) {
                launchingTaskCount++;
            }
            if (launchingTaskCount >= numberOfTasks) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTaskLaunching(Map<String, Task> tasksById, TaskRequest request) {
        Task current = tasksById.get(request.getId());
        if (current == null) {
            return false;
        }
        TaskState state = current.getStatus().getState();
        return state == TaskState.Accepted || state == TaskState.Launched || state == TaskState.StartInitiated;
    }

    public static Set<String> getTolerations(V3QueueableTask taskRequest) {
        String jobTolerationValue = nonNull(
                (String) taskRequest.getJob().getJobDescriptor().getAttributes().get(JOB_PARAMETER_ATTRIBUTES_TOLERATIONS)
        ).toLowerCase();
        return StringExt.splitByCommaIntoSet(jobTolerationValue);
    }

    public static Set<String> getTaints(AgentInstanceGroup instanceGroup, AgentInstance instance) {
        String instanceGroupTaintsValue = nonNull(instanceGroup.getAttributes().get(TAINTS)).toLowerCase();
        Set<String> instanceGroupTaints = StringExt.splitByCommaIntoSet(instanceGroupTaintsValue);

        String instanceTaintsValue = nonNull(instance.getAttributes().get(TAINTS)).toLowerCase();
        Set<String> instanceTaints = StringExt.splitByCommaIntoSet(instanceTaintsValue);

        return CollectionsExt.merge(instanceGroupTaints, instanceTaints);
    }

    public static Map<String, String> applicationAndCapacityGroupTags(V3QueueableTask task) {
        JobDescriptor<?> jobDescriptor = task.getJob().getJobDescriptor();
        int tierNumber = task.getQAttributes().getTierNumber();
        String tier = tierNumber >= 0 && tierNumber < Tier.values().length ?
                Tier.values()[tierNumber].name().toLowerCase() : "unknown";
        return ImmutableMap.<String, String>builder()
                .put("applicationName", jobDescriptor.getApplicationName())
                .put("capacityGroup", jobDescriptor.getCapacityGroup())
                .put("tier", tier)
                .build();
    }
}
