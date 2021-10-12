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

package com.netflix.titus.master.jobmanager.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.NetworkConfiguration.NetworkMode;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {

    private JobManagerUtil() {
    }

    public static Set<String> filterActiveTaskIds(ReconciliationEngine<JobManagerReconcilerEvent> engine) {
        Set<String> result = new HashSet<>();
        for (EntityHolder taskHolder : engine.getRunningView().getChildren()) {
            Task task = taskHolder.getEntity();
            TaskState state = task.getStatus().getState();
            if (state != TaskState.Finished) {
                result.add(task.getId());
            }
        }
        return result;
    }

    public static Pair<Tier, String> getTierAssignment(Job job, ApplicationSlaManagementService capacityGroupService) {
        return getTierAssignment(job.getJobDescriptor(), capacityGroupService);
    }

    public static Pair<Tier, String> getTierAssignment(JobDescriptor<?> jobDescriptor, ApplicationSlaManagementService capacityGroupService) {
        String capacityGroup = jobDescriptor.getCapacityGroup();

        ApplicationSLA applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        if (applicationSLA == null) {
            capacityGroup = ApplicationSlaManagementService.DEFAULT_APPLICATION;
            applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        }

        return Pair.of(applicationSLA.getTier(), capacityGroup);
    }

    public static ApplicationSLA getCapacityGroupDescriptor(JobDescriptor<?> jobDescriptor, ApplicationSlaManagementService capacityGroupService) {
        String capacityGroup = jobDescriptor.getCapacityGroup();

        ApplicationSLA applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        return applicationSLA == null ? capacityGroupService.getApplicationSLA(ApplicationSlaManagementService.DEFAULT_APPLICATION) : applicationSLA;
    }

    public static String getCapacityGroupDescriptorName(JobDescriptor<?> jobDescriptor, ApplicationSlaManagementService capacityGroupService) {
        ApplicationSLA applicationSLA = getCapacityGroupDescriptor(jobDescriptor, capacityGroupService);
        return applicationSLA == null ? ApplicationSlaManagementService.DEFAULT_APPLICATION : applicationSLA.getAppName();
    }

    public static Function<Task, Optional<Task>> newMesosTaskStateUpdater(TaskStatus newTaskStatus, Optional<TitusExecutorDetails> detailsOpt, TitusRuntime titusRuntime) {
        return oldTask -> {
            TaskState oldState = oldTask.getStatus().getState();
            TaskState newState = newTaskStatus.getState();

            // De-duplicate task status updates. 'Launched' state is reported from two places, so we get
            // 'Launched' state update twice. For other states there may be multiple updates, each with different reason.
            // For example in 'StartInitiated', multiple updates are send reporting progress of a container setup.
            if (TaskStatus.areEquivalent(newTaskStatus, oldTask.getStatus()) && !detailsOpt.isPresent()) {
                return Optional.empty();
            }
            if (newState == oldState && newState == TaskState.Launched) {
                return Optional.empty();
            }
            // Sanity check. If we got earlier task state, it is state model invariant violation.
            if (TaskState.isBefore(newState, oldState)) {
                titusRuntime.getCodeInvariants().inconsistent("Received task state update to a previous state: taskId=%s, previous=%s, current=%s", oldTask.getId(), oldState, newState);
                return Optional.empty();
            }

            Task newTask = JobFunctions.changeTaskStatus(oldTask, newTaskStatus);
            Task newTaskWithPlacementData = attachTitusExecutorNetworkData(newTask, detailsOpt);
            return Optional.of(newTaskWithPlacementData);
        };
    }

    public static Task attachTitusExecutorNetworkData(Task task, Optional<TitusExecutorDetails> detailsOpt) {
        return detailsOpt.map(details -> {
            TitusExecutorDetails.NetworkConfiguration networkConfiguration = details.getNetworkConfiguration();
            if (networkConfiguration != null) {
                String ipv4 = networkConfiguration.getIpAddress();
                String ipv6 = networkConfiguration.getIpV6Address();
                String primaryIP = networkConfiguration.getPrimaryIpAddress();
                Map<String, String> newContext = new HashMap<>(task.getTaskContext());
                BiConsumer<String, String> contextSetter = (key, value) -> StringExt.applyIfNonEmpty(value, v -> newContext.put(key, v));

                if (networkConfiguration.getNetworkMode().equals(NetworkMode.Ipv6AndIpv4Fallback.toString())) {
                    // In the IPV6_ONLY_WITH_TRANSITION mode, the ipv4 on the pod doesn't represent
                    // a unique IP for that pod, but a shared one. This should not be consumed
                    // as a normal ip by normal tools, and deserves a special attribute
                    contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_TRANSITION_IPV4, ipv4);
                } else {
                    contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV4, ipv4);
                }
                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, primaryIP);
                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV6, ipv6);
                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_ID, networkConfiguration.getEniID());
                parseEniResourceId(networkConfiguration.getResourceID()).ifPresent(index -> newContext.put(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX, index));

                return task.toBuilder().addAllToTaskContext(newContext).build();
            }
            return task;
        }).orElse(task);
    }

    public static Task attachKubeletNetworkData(Task task, PodWrapper podWrapper) {
        if (podWrapper.getV1Pod().getStatus() == null) {
            return task;
        }
        String ipv4 = podWrapper.getV1Pod().getStatus().getPodIP();
        if (ipv4 == null || ipv4.isEmpty()) {
            return task;
        }

        Map<String, String> newContext = new HashMap<>(task.getTaskContext());
        BiConsumer<String, String> contextSetter = (key, value) -> StringExt.applyIfNonEmpty(value, v -> newContext.put(key, v));
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, ipv4);
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV4, ipv4);
        return task.toBuilder().addAllToTaskContext(newContext).build();
    }

    public static Optional<String> parseEniResourceId(String resourceId) {
        if (resourceId == null || !resourceId.startsWith("resource-eni-")) {
            return Optional.empty();
        }
        return Optional.of(resourceId.substring("resource-eni-".length()));
    }

    /**
     * @return {@link Optional#empty()} when no binpacking should be applied for task relocation purposes
     */
    public static Optional<String> getRelocationBinpackMode(Job<?> job) {
        return job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy
                ? Optional.of("SelfManaged")
                : Optional.empty();
    }
}
