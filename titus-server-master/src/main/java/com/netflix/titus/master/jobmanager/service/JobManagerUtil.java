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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.archaius.api.Config;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodWrapper;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.apache.mesos.Protos;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {
    private static final ObjectMapper mapper = ObjectMappers.defaultMapper();
    private static final String EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX = "titusMaster.jobManager";

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
            Task newTaskWithPlacementData = attachTitusExecutorData(newTask, detailsOpt);
            return Optional.of(newTaskWithPlacementData);
        };
    }

    public static Task attachTitusExecutorData(Task task, Optional<TitusExecutorDetails> detailsOpt) {
        return detailsOpt.map(details -> {
            TitusExecutorDetails.NetworkConfiguration networkConfiguration = details.getNetworkConfiguration();
            if (networkConfiguration != null) {
                Map<String, String> newContext = new HashMap<>(task.getTaskContext());
                BiConsumer<String, String> contextSetter = (key, value) -> StringExt.applyIfNonEmpty(value, v -> newContext.put(key, v));

                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, networkConfiguration.getIpAddress());
                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV4, networkConfiguration.getIpAddress());
                Evaluators.acceptNotNull(networkConfiguration.getIpV6Address(), ipv6 -> newContext.put(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV6, ipv6));
                contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_ID, networkConfiguration.getEniID());
                parseEniResourceId(networkConfiguration.getResourceID()).ifPresent(index -> newContext.put(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX, index));

                return task.toBuilder().addAllToTaskContext(newContext).build();
            }
            return task;
        }).orElse(task);
    }

    public static Task attachKubeletData(Task task, PodWrapper podWrapper) {
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

    public static Function<Task, Task> newTaskLaunchConfigurationUpdater(String zoneAttributeName,
                                                                         VirtualMachineLease lease,
                                                                         PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult,
                                                                         Optional<String> executorUriOverrideOpt,
                                                                         Map<String, String> attributesMap,
                                                                         Map<String, String> opportunisticResourcesContext,
                                                                         String tier,
                                                                         TitusRuntime titusRuntime) {
        return oldTask -> {
            if (oldTask.getStatus().getState() != TaskState.Accepted) {
                throw JobManagerException.unexpectedTaskState(oldTask, TaskState.Accepted);
            }

            Map<String, String> taskContext = new HashMap<>(opportunisticResourcesContext);
            Map<String, Protos.Attribute> attributes = CollectionsExt.nonNull(lease.getAttributeMap());
            String hostIp = findAttribute(attributes, "hostIp").orElse(lease.hostname());
            taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, hostIp);
            taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST_IP, hostIp);

            executorUriOverrideOpt.ifPresent(v -> taskContext.put(TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE, v));
            taskContext.put(TaskAttributes.TASK_ATTRIBUTES_TIER, tier);

            if (!attributes.isEmpty()) {
                attributesMap.forEach((k, v) -> taskContext.put("agent." + k, v));

                // TODO Some agent attribute names are configurable, some not. We need to clean this up.
                findAttribute(attributes, zoneAttributeName).ifPresent(value ->
                        taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, value)
                );
                findAttribute(attributes, "id").ifPresent(value ->
                        taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, value)
                );
            }

            TaskStatus taskStatus = JobModel.newTaskStatus()
                    .withState(TaskState.Launched)
                    .withReasonCode("scheduled")
                    .withReasonMessage("Fenzo task placement on node " + TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID + ". Next it needs to start.")
                    .withTimestamp(titusRuntime.getClock().wallTime())
                    .build();

            TwoLevelResource twoLevelResource = TwoLevelResource.newBuilder()
                    .withName(consumeResult.getAttrName())
                    .withValue(consumeResult.getResName())
                    .withIndex(consumeResult.getIndex())
                    .build();

            return JobFunctions.addAllocatedResourcesToTask(oldTask, taskStatus, twoLevelResource, taskContext);
        };
    }

    public static Optional<TitusExecutorDetails> parseDetails(String statusData) {
        if (StringExt.isEmpty(statusData)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(statusData, TitusExecutorDetails.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Optional<String> getExecutorUriOverride(Config config,
                                                          Map<String, String> attributesMap) {
        String ami = attributesMap.getOrDefault("ami", "defaultAmi");
        String amiExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".amiExecutorUriOverride." + ami, "");
        if (!Strings.isNullOrEmpty(amiExecutorUriOverride)) {
            return Optional.of(amiExecutorUriOverride);
        }

        String asg = attributesMap.getOrDefault("asg", "defaultAsg");
        String asgExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".asgExecutorUriOverride." + asg, "");
        if (!Strings.isNullOrEmpty(asgExecutorUriOverride)) {
            return Optional.of(asgExecutorUriOverride);
        }

        String instance = attributesMap.getOrDefault("id", "defaultInstance");
        String instanceExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".instanceExecutorUriOverride." + instance, "");
        if (!Strings.isNullOrEmpty(instanceExecutorUriOverride)) {
            return Optional.of(instanceExecutorUriOverride);
        }

        return Optional.empty();
    }

    private static Optional<String> findAttribute(Map<String, Protos.Attribute> attributes, String name) {
        Protos.Attribute attribute = attributes.get(name);
        return (attribute != null) ? Optional.of(attribute.getText().getValue()) : Optional.empty();
    }
}
