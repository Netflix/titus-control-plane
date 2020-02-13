/*
 * Copyright 2020 Netflix, Inc.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.models.V1ContainerState;
import io.kubernetes.client.models.V1ContainerStateTerminated;
import io.kubernetes.client.models.V1Pod;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import static com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;

/**
 * TODO Incorporate this into {@link DefaultV3JobOperations} once Fenzo is removed.
 */
@Singleton
public class KubeNotificationProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KubeNotificationProcessor.class);

    private static CallMetadata KUBE_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Kube").build();

    private final DirectKubeApiServerIntegrator kubeApiServerIntegrator;
    private final V3JobOperations v3JobOperations;

    private Disposable subscription;

    @Inject
    public KubeNotificationProcessor(DirectKubeApiServerIntegrator kubeApiServerIntegrator,
                                     V3JobOperations v3JobOperations) {
        this.kubeApiServerIntegrator = kubeApiServerIntegrator;
        this.v3JobOperations = v3JobOperations;
    }

    @Activator
    public void enterActiveMode() {
        this.subscription = kubeApiServerIntegrator.events()
                .flatMap(event -> {
                    Pair<Job<?>, Task> jobAndTask = v3JobOperations.findTaskById(event.getTaskId()).orElse(null);
                    if (jobAndTask == null) {
                        logger.warn("Got Kube notification about unknown task: {}", event.getTaskId());
                        return Mono.empty();
                    }
                    return handlePodUpdatedEvent(event, jobAndTask.getLeft(), jobAndTask.getRight());
                })
                .ignoreElements()
                .doOnError(error -> logger.warn("Kube integration event stream terminated with an error (retrying soon)", error))
                .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .subscribe(
                        next -> {
                            // Nothing
                        },
                        e -> logger.error("Event stream terminated"),
                        () -> logger.info("Event stream completed")
                );
    }

    public void shutdown() {
        ReactorExt.safeDispose(subscription);
    }

    private Mono<Void> handlePodUpdatedEvent(PodEvent event, Job job, Task task) {
        if (event.getPod() == null || event.getPod().getStatus() == null || CollectionsExt.isNullOrEmpty(event.getPod().getStatus().getContainerStatuses())) {
            logger.info("Pod notification, but no container info yet: {}", task.getId());
            return Mono.empty();
        }

        V1ContainerState containerState = event.getPod().getStatus().getContainerStatuses().get(0).getState();
        TaskState taskState = task.getStatus().getState();
        Optional<TitusExecutorDetails> executorDetailsOpt = KubeUtil.getTitusExecutorDetails(event.getPod());

        logger.info("State: {}", containerState);
        if (containerState.getWaiting() != null) {
            logger.debug("Ignoring 'waiting' state update, as task must stay in the 'Accepted' state here");
        } else if (containerState.getRunning() != null) {
            if (TaskState.isBefore(taskState, TaskState.Started)) {
                return updateTaskStatus(event.getPod(), task, TaskState.Started, executorDetailsOpt);
            }
        } else if (containerState.getTerminated() != null) {
            if (taskState != TaskState.Finished) {
                return updateTaskStatus(event.getPod(), task, TaskState.Finished, executorDetailsOpt);
            }
        }
        return Mono.empty();
    }

    private Mono<Void> updateTaskStatus(V1Pod pod,
                                        Task task,
                                        TaskState newTaskState,
                                        Optional<TitusExecutorDetails> executorDetailsOpt) {
        return ReactorExt.toMono(v3JobOperations.updateTask(
                task.getId(),
                currentTask -> {
                    TaskStatus newStatus = TaskStatus.newBuilder()
                            .withState(newTaskState)
                            .withReasonCode(TaskStatus.REASON_NORMAL)
                            .withReasonMessage("Kube pod notification")
                            .build();

                    List<TaskStatus> newHistory = CollectionsExt.copyAndAdd(currentTask.getStatusHistory(), currentTask.getStatus());

                    Task updatedTask = currentTask.toBuilder()
                            .withStatus(newStatus)
                            .withStatusHistory(newHistory)
                            .build();


                    Task fixedTask = fillInMissingStates(pod, updatedTask);
                    Task taskWithPlacementData = JobManagerUtil.attachPlacementData(fixedTask, executorDetailsOpt);

                    return Optional.of(taskWithPlacementData);
                },
                V3JobOperations.Trigger.Kube,
                "Kube pod notification",
                KUBE_CALL_METADATA
        ));
    }

    private Task fillInMissingStates(V1Pod pod, Task task) {
        TaskState currentState = task.getStatus().getState();
        if (currentState != Finished) {
            return task;
        }

        Optional<V1ContainerStateTerminated> terminatedContainerStatusOpt = KubeUtil.findTerminatedContainerStatus(pod);
        if (!terminatedContainerStatusOpt.isPresent()) {
            return task;
        }

        V1ContainerStateTerminated containerStateTerminated = terminatedContainerStatusOpt.get();
        DateTime startedAt = containerStateTerminated.getStartedAt();
        if (startedAt == null) {
            return task;
        }

        String taskId = task.getId();

        List<TaskStatus> missingStatuses = new ArrayList<>();

        TaskStatus.Builder statusTemplate = TaskStatus.newBuilder()
                .withReasonCode(TaskStatus.REASON_STATE_MISSING)
                .withReasonMessage("Filled in")
                .withTimestamp(startedAt.getMillis());

        Optional<TaskStatus> startInitiatedOpt = JobFunctions.findTaskStatus(task, TaskState.StartInitiated);
        if (!startInitiatedOpt.isPresent()) {
            logger.debug("Adding missing task status: StartInitiated for task: {}", taskId);
            missingStatuses.add(statusTemplate.withState(TaskState.StartInitiated).build());
        }
        Optional<TaskStatus> startedOpt = JobFunctions.findTaskStatus(task, Started);
        if (!startedOpt.isPresent()) {
            logger.debug("Publishing missing task status: Started for task: {}", taskId);
            missingStatuses.add(statusTemplate.withState(Started).build());
        }

        if (missingStatuses.isEmpty()) {
            return task;
        }

        List<TaskStatus> newStatusHistory = new ArrayList<>(task.getStatusHistory());
        newStatusHistory.addAll(missingStatuses);
        newStatusHistory.sort(Comparator.comparing(ExecutableStatus::getState));

        return task.toBuilder().withStatusHistory(newStatusHistory).build();

    }
}
