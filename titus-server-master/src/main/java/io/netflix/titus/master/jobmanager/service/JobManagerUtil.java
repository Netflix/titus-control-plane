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
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VirtualMachineLease;
import io.netflix.titus.api.jobmanager.model.job.Job;
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
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.apache.mesos.Protos;

import static io.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {
    private static final ObjectMapper mapper = ObjectMappers.defaultMapper();

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

    public static Pair<Tier, String> getTierAssignment(Job<?> job, ApplicationSlaManagementService capacityGroupService) {
        String capacityGroup = job.getJobDescriptor().getCapacityGroup();

        ApplicationSLA applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        if (applicationSLA == null) {
            capacityGroup = ApplicationSlaManagementService.DEFAULT_APPLICATION;
            applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        }

        return Pair.of(applicationSLA.getTier(), capacityGroup);
    }

    public static Function<Task, Task> newTaskStateUpdater(TaskStatus taskStatus, String statusData) {
        return oldTask -> {
            // De-duplicate task status updates. For example 'Launched' state is reported from two places, so we got
            // 'Launched' state update twice.
            if (!TaskState.isBefore(oldTask.getStatus().getState(), taskStatus.getState())) {
                return oldTask;
            }

            final Task newTask = JobFunctions.changeTaskStatus(oldTask, taskStatus);
            return parseDetails(statusData).map(details -> {
                if (details.getNetworkConfiguration() != null && !StringExt.isEmpty(details.getNetworkConfiguration().getIpAddress())) {
                    return newTask.toBuilder()
                            .withTaskContext(CollectionsExt.copyAndAdd(
                                    newTask.getTaskContext(),
                                    TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, details.getNetworkConfiguration().getIpAddress()
                            )).build();
                }
                return newTask;
            }).orElse(newTask);
        };
    }

    public static Function<Task, Task> newTaskLaunchConfigurationUpdater(String zoneAttributeName,
                                                                         VirtualMachineLease lease,
                                                                         PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult,
                                                                         Map<String, String> attributesMap) {
        return oldTask -> {
            Map<String, String> taskContext = new HashMap<>();
            taskContext.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, lease.hostname());

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

    private static Optional<TitusExecutorDetails> parseDetails(String statusData) {
        if (StringExt.isEmpty(statusData)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(statusData, TitusExecutorDetails.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> addAttributeToContext(Map<String, Protos.Attribute> attributes, String name) {
        Protos.Attribute attribute = attributes.get(name);
        return (attribute != null) ? Optional.of(attribute.getText().getValue()) : Optional.empty();
    }
}
