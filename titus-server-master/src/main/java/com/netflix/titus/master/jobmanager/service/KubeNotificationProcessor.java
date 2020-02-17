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
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodNotFoundEvent;
import io.kubernetes.client.models.V1ContainerState;
import io.kubernetes.client.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * TODO Incorporate this into {@link DefaultV3JobOperations} once Fenzo is removed.
 * TODO Add junit tests
 */
@Singleton
public class KubeNotificationProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KubeNotificationProcessor.class);

    private static CallMetadata KUBE_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Kube").build();

    private final JobManagerConfiguration configuration;
    private final DirectKubeApiServerIntegrator kubeApiServerIntegrator;
    private final V3JobOperations v3JobOperations;

    private Disposable subscription;

    @Inject
    public KubeNotificationProcessor(JobManagerConfiguration configuration,
                                     DirectKubeApiServerIntegrator kubeApiServerIntegrator,
                                     V3JobOperations v3JobOperations) {
        this.configuration = configuration;
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

                    Task task = jobAndTask.getRight();
                    if (!JobFunctions.isOwnedByKubeScheduler(task)) {
                        logger.debug("Ignoring notification for task managed via Mesos adapter: taskId={}", task.getId());
                        return Mono.empty();
                    }

                    if (event instanceof PodNotFoundEvent) {
                        return handlePodNotFoundEvent((PodNotFoundEvent) event);
                    }
                    // We depend in this flatmap on the fact that the task update event is added by the source thread to
                    // the job reconciler queue. This guarantees the right order of the execution.
                    // TODO Implement flatMapWithSequentialSubscription operator
                    return handlePodUpdatedEvent(event, jobAndTask.getLeft(), task);
                }, Math.max(1, configuration.getKubeEventConcurrencyLimit()))
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

    private Mono<Void> handlePodNotFoundEvent(PodNotFoundEvent event) {
        Task task = event.getTask();
        return ReactorExt.toMono(v3JobOperations.updateTask(
                task.getId(),
                currentTask -> {
                    TaskStatus newStatus = TaskStatus.newBuilder()
                            .withState(TaskState.Finished)
                            .withReasonCode(TaskStatus.REASON_TASK_LOST)
                            .withReasonMessage("Pod not found")
                            .build();

                    List<TaskStatus> newHistory = CollectionsExt.copyAndAdd(currentTask.getStatusHistory(), currentTask.getStatus());

                    Task updatedTask = currentTask.toBuilder()
                            .withStatus(newStatus)
                            .withStatusHistory(newHistory)
                            .build();

                    return Optional.of(updatedTask);
                },
                V3JobOperations.Trigger.Kube,
                "Kube pod notification",
                KUBE_CALL_METADATA
        ));
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
        if (currentState != TaskState.Started && currentState != TaskState.Finished) {
            return task;
        }

        V1ContainerState containerState = KubeUtil.findContainerState(pod).orElse(null);
        if (containerState == null) {
            return task;
        }

        long startAtTimestamp;
        if (currentState == TaskState.Started) {
            if (containerState.getRunning() == null || containerState.getRunning().getStartedAt() == null) {
                return task;
            }
            startAtTimestamp = containerState.getRunning().getStartedAt().getMillis();
        } else { // TaskState.Finished
            if (containerState.getTerminated() == null || containerState.getTerminated().getStartedAt() == null) {
                return task;
            }
            startAtTimestamp = containerState.getTerminated().getStartedAt().getMillis();
        }

        TaskStatus.Builder statusTemplate = TaskStatus.newBuilder()
                .withReasonCode(TaskStatus.REASON_STATE_MISSING)
                .withReasonMessage("Filled in")
                .withTimestamp(startAtTimestamp);

        List<TaskStatus> missingStatuses = new ArrayList<>();
        addIfMissing(task, TaskState.Launched, statusTemplate).ifPresent(missingStatuses::add);
        addIfMissing(task, TaskState.StartInitiated, statusTemplate).ifPresent(missingStatuses::add);
        addIfMissing(task, TaskState.Started, statusTemplate).ifPresent(missingStatuses::add);

        if (missingStatuses.isEmpty()) {
            return task;
        }

        List<TaskStatus> newStatusHistory = new ArrayList<>(task.getStatusHistory());
        newStatusHistory.addAll(missingStatuses);
        newStatusHistory.sort(Comparator.comparing(ExecutableStatus::getState));

        return task.toBuilder().withStatusHistory(newStatusHistory).build();

    }

    private Optional<TaskStatus> addIfMissing(Task task, TaskState expectedState, TaskStatus.Builder statusTemplate) {
        Optional<TaskStatus> foundStatus = JobFunctions.findTaskStatus(task, expectedState);
        if (!foundStatus.isPresent()) {
            logger.debug("Adding missing task status: {} for task: {}", expectedState, task.getId());
            return Optional.of(statusTemplate.withState(expectedState).build());
        }
        return Optional.empty();
    }
}
