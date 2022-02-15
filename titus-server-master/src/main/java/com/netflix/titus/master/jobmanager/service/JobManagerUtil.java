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
import java.util.Map;
import java.util.Optional;
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
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.NetworkConfiguration;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;

import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_ELASTIC_IP_ADDRESS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_ENI_ID;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_ENI_IP_ADDRESS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_ENI_IPV6_ADDRESS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_IP_ADDRESS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LEGACY_ANNOTATION_NETWORK_MODE;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {

    private JobManagerUtil() {
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

    public static Function<Task, Optional<Task>> newMesosTaskStateUpdater(TaskStatus newTaskStatus, TitusRuntime titusRuntime) {
        return oldTask -> {
            TaskState oldState = oldTask.getStatus().getState();
            TaskState newState = newTaskStatus.getState();

            // De-duplicate task status updates. 'Launched' state is reported from two places, so we get
            // 'Launched' state update twice. For other states there may be multiple updates, each with different reason.
            // For example in 'StartInitiated', multiple updates are send reporting progress of a container setup.
            if (TaskStatus.areEquivalent(newTaskStatus, oldTask.getStatus())) {
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
            return Optional.of(newTask);
        };
    }

    public static Task attachNetworkDataFromPod(Task task, PodWrapper podWrapper) {
        Map<String, String> annotations = podWrapper.getV1Pod().getMetadata().getAnnotations();
        if (annotations == null) {
            return task;
        }

        String ipaddress = annotations.get(LEGACY_ANNOTATION_IP_ADDRESS);
        String elasticIPAddress = annotations.get(LEGACY_ANNOTATION_ELASTIC_IP_ADDRESS);
        String eniIPAddress = annotations.get(LEGACY_ANNOTATION_ENI_IP_ADDRESS);
        String eniIPv6Address = annotations.get(LEGACY_ANNOTATION_ENI_IPV6_ADDRESS);
        String effectiveNetworkMode = annotations.get(LEGACY_ANNOTATION_NETWORK_MODE);
        String eniID = annotations.get(LEGACY_ANNOTATION_ENI_ID);

        Map<String, String> newContext = new HashMap<>(task.getTaskContext());
        BiConsumer<String, String> contextSetter = (key, value) -> StringExt.applyIfNonEmpty(value, v -> newContext.put(key, v));
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, ipaddress);
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV6, eniIPv6Address);
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_ID, eniID);

        if (effectiveNetworkMode != null && effectiveNetworkMode.equals(NetworkConfiguration.NetworkMode.Ipv6AndIpv4Fallback.toString())) {
            // In the IPV6_ONLY_WITH_TRANSITION mode, the ipv4 on the pod doesn't represent
            // a unique IP for that pod, but a shared one. This should not be consumed
            // as a normal ip by normal tools, and deserves a special attribute
            contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_TRANSITION_IPV4, eniIPAddress);
        }
        if (effectiveNetworkMode != null && effectiveNetworkMode != NetworkConfiguration.NetworkMode.Ipv6Only.toString() && effectiveNetworkMode != NetworkConfiguration.NetworkMode.Ipv6AndIpv4Fallback.toString()) {
            // Only in the case of non IPv6-only mode do we want to set this attribute
            contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV4, eniIPAddress);
        }

        //If we did assign an EIP, we set that as the "main" ip address
        contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, elasticIPAddress);

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
