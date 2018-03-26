/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.archaius.api.Config;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VirtualMachineLease;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.apache.mesos.Protos;

import static io.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE;
import static io.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;
import static io.netflix.titus.common.util.code.CodeInvariants.codeInvariants;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {
    private static final ObjectMapper mapper = ObjectMappers.defaultMapper();
    private static final String EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX = "titusMaster.jobManager";

    private JobManagerUtil() {
    }

    public static Set<String> filterActiveTaskIds(ReconciliationEngine<JobManagerReconcilerEvent> engine) {
        return engine.getRunningView().getChildren().stream()
                .map(taskHolder -> {
                    Task task = taskHolder.getEntity();
                    TaskState state = task.getStatus().getState();
                    return state != TaskState.Finished ? task.getId() : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
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

    public static Function<Task, Optional<Task>> newMesosTaskStateUpdater(TaskStatus newTaskStatus, Optional<TitusExecutorDetails> detailsOpt) {
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
                codeInvariants().inconsistent("Received task state update to a previous state: taskId=%s, previous=%s, current=%s", oldTask.getId(), oldState, newState);
                return Optional.empty();
            }

            final Task newTask = JobFunctions.changeTaskStatus(oldTask, newTaskStatus);
            Task newTaskWithPlacementData = detailsOpt.map(details -> {
                if (details.getNetworkConfiguration() != null) {

                    Map<String, String> newContext = new HashMap<>(newTask.getTaskContext());
                    BiConsumer<String, String> contextSetter = (key, value) -> StringExt.applyIfNonEmpty(value, v -> newContext.put(key, v));

                    contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, details.getNetworkConfiguration().getIpAddress());
                    contextSetter.accept(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_ID, details.getNetworkConfiguration().getEniID());
                    parseEniResourceId(details.getNetworkConfiguration().getResourceID()).ifPresent(index -> newContext.put(TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX, index));

                    return newTask.toBuilder().addAllToTaskContext(newContext).build();
                }
                return newTask;
            }).orElse(newTask);
            return Optional.of(newTaskWithPlacementData);
        };
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
                                                                         Map<String, String> attributesMap) {
        return oldTask -> {
            Map<String, String> taskContext = new HashMap<>();
            taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, lease.hostname());
            executorUriOverrideOpt.ifPresent(v -> taskContext.put(TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE, v));

            Map<String, Protos.Attribute> attributes = lease.getAttributeMap();
            if (!isNullOrEmpty(attributes)) {
                attributesMap.forEach((k, v) -> taskContext.put("agent." + k, v));

                // TODO Some agent attribute names are configurable, some not. We need to clean this up.
                addAttributeToContext(attributes, zoneAttributeName).ifPresent(value ->
                        taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, value)
                );
                addAttributeToContext(attributes, "id").ifPresent(value ->
                        taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, value)
                );
            }

            TaskStatus taskStatus = JobModel.newTaskStatus()
                    .withState(TaskState.Launched)
                    .withReasonCode("scheduled")
                    .withReasonMessage("Fenzo task placement")
                    .build();

            TwoLevelResource twoLevelResource = TwoLevelResource.newBuilder()
                    .withName(consumeResult.getAttrName())
                    .withValue(consumeResult.getResName())
                    .withIndex(consumeResult.getIndex())
                    .build();

            if (oldTask.getStatus().getState() != TaskState.Accepted) {
                throw JobManagerException.unexpectedTaskState(oldTask, TaskState.Accepted);
            }
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
        String ami = attributesMap.getOrDefault("ami", "");
        String amiExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".amiExecutorUriOverride." + ami, "");
        if (!Strings.isNullOrEmpty(amiExecutorUriOverride)) {
            return Optional.of(amiExecutorUriOverride);
        }

        String asg = attributesMap.getOrDefault("asg", "");
        String asgExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".asgExecutorUriOverride." + asg, "");
        if (!Strings.isNullOrEmpty(asgExecutorUriOverride)) {
            return Optional.of(asgExecutorUriOverride);
        }

        String host = attributesMap.getOrDefault("id", "");
        String hostExecutorUriOverride = config.getString(EXECUTOR_URI_OVERRIDE_PROPERTY_PREFIX + ".hostExecutorUriOverride." + host, "");
        if (!Strings.isNullOrEmpty(hostExecutorUriOverride)) {
            return Optional.of(hostExecutorUriOverride);
        }

        return Optional.empty();
    }

    private static Optional<String> addAttributeToContext(Map<String, Protos.Attribute> attributes, String name) {
        Protos.Attribute attribute = attributes.get(name);
        return (attribute != null) ? Optional.of(attribute.getText().getValue()) : Optional.empty();
    }
}
