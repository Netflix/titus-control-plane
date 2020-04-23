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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.kubeapiserver.KubeConstants;
import com.netflix.titus.master.mesos.kubeapiserver.KubeJobManagementReconciler;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodDeletedEvent;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodNotFoundEvent;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodUpdatedEvent;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.common.util.Evaluators.acceptNotNull;
import static com.netflix.titus.master.mesos.kubeapiserver.KubeConstants.NODE_ANNOTATION_PREFIX;

/**
 * TODO Incorporate this into {@link DefaultV3JobOperations} once Fenzo is removed.
 * TODO Add junit tests
 */
@Singleton
public class KubeNotificationProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KubeNotificationProcessor.class);
    private static final CallMetadata KUBE_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Kube").build();

    private final JobManagerConfiguration configuration;
    private final DirectKubeApiServerIntegrator kubeApiServerIntegrator;
    private final KubeJobManagementReconciler kubeJobManagementReconciler;
    private final V3JobOperations v3JobOperations;

    private Disposable subscription;

    @Inject
    public KubeNotificationProcessor(JobManagerConfiguration configuration,
                                     DirectKubeApiServerIntegrator kubeApiServerIntegrator,
                                     KubeJobManagementReconciler kubeJobManagementReconciler,
                                     V3JobOperations v3JobOperations) {
        this.configuration = configuration;
        this.kubeApiServerIntegrator = kubeApiServerIntegrator;
        this.kubeJobManagementReconciler = kubeJobManagementReconciler;
        this.v3JobOperations = v3JobOperations;
    }

    @Activator
    public void enterActiveMode() {
        this.subscription = kubeApiServerIntegrator.events().mergeWith(kubeJobManagementReconciler.getPodEventSource())
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

        Optional<V1Node> node;
        if (event instanceof PodUpdatedEvent) {
            node = ((PodUpdatedEvent) event).getNode();
        } else if (event instanceof PodDeletedEvent) {
            node = ((PodDeletedEvent) event).getNode();
        } else {
            node = Optional.empty();
        }

        logger.info("Pod state change event: taskId={}, details={}", task.getId(), KubeUtil.formatV1ContainerState(containerState));
        if (containerState.getWaiting() != null) {
            // TODO: check for updated annotations and update tasks without having to wait for a containerState transition
            logger.debug("Ignoring 'waiting' state update, as task must stay in the 'Accepted' state here");
        } else if (containerState.getRunning() != null) {
            if (TaskState.isBefore(taskState, TaskState.Started)) {
                return updateTaskStatus(event.getPod(), task, TaskState.Started, executorDetailsOpt, node);
            }
        } else if (containerState.getTerminated() != null) {
            if (taskState != TaskState.Finished) {
                return updateTaskStatus(event.getPod(), task, TaskState.Finished, executorDetailsOpt, node);
            }
        }
        return Mono.empty();
    }

    private Mono<Void> handlePodNotFoundEvent(PodNotFoundEvent event) {
        Task task = event.getTask();

        logger.info("Pod not found event: taskId={}, finalTaskStatus={}", task.getId(), event.getFinalTaskStatus());

        return ReactorExt.toMono(v3JobOperations.updateTask(
                task.getId(),
                currentTask -> {
                    List<TaskStatus> newHistory = CollectionsExt.copyAndAdd(currentTask.getStatusHistory(), currentTask.getStatus());

                    Task updatedTask = currentTask.toBuilder()
                            .withStatus(event.getFinalTaskStatus())
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
                                        Optional<TitusExecutorDetails> executorDetailsOpt,
                                        Optional<V1Node> node) {
        return ReactorExt.toMono(v3JobOperations.updateTask(
                task.getId(),
                currentTask -> {
                    TaskStatus newStatus;

                    if (newTaskState != TaskState.Finished) {
                        newStatus = TaskStatus.newBuilder()
                                .withState(newTaskState)
                                .withReasonCode(TaskStatus.REASON_NORMAL)
                                .withReasonMessage("Kube pod notification")
                                .build();
                    } else {
                        TaskStatus.Builder newStatusBuilder = TaskStatus.newBuilder().withState(TaskState.Finished);

                        boolean hasDeletionTimestamp = pod.getMetadata().getDeletionTimestamp() != null;

                        if ("failed".equalsIgnoreCase(pod.getStatus().getPhase())) {
                            newStatusBuilder
                                    .withReasonCode(hasDeletionTimestamp ? REASON_TASK_KILLED : TaskStatus.REASON_FAILED)
                                    .withReasonMessage(Evaluators.getOrDefault(pod.getStatus().getMessage(), "Pod execution failed"));
                        } else {
                            newStatusBuilder
                                    .withReasonCode(hasDeletionTimestamp ? REASON_TASK_KILLED : REASON_NORMAL)
                                    .withReasonMessage("Kube pod notification");
                        }

                        newStatus = newStatusBuilder.build();
                    }

                    List<TaskStatus> newHistory = CollectionsExt.copyAndAdd(currentTask.getStatusHistory(), currentTask.getStatus());

                    Task updatedTask = currentTask.toBuilder()
                            .withStatus(newStatus)
                            .withStatusHistory(newHistory)
                            .build();

                    Task fixedTask = fillInMissingStates(pod, updatedTask);
                    Task taskWithPlacementData = JobManagerUtil.attachPlacementData(fixedTask, executorDetailsOpt);
                    Task taskWithNodeMetadata = node.map(n -> attachNodeMetadata(taskWithPlacementData, n)).orElse(taskWithPlacementData);
                    Task taskWithAnnotations = addMissingAttributes(pod, updatedTask);

                    return Optional.of(taskWithAnnotations);
                },
                V3JobOperations.Trigger.Kube,
                "Kube pod notification",
                KUBE_CALL_METADATA
        ));
    }

    private Task attachNodeMetadata(Task task, V1Node node) {
        Map<String, String> annotations = node.getMetadata().getAnnotations();
        if (CollectionsExt.isNullOrEmpty(annotations)) {
            return task;
        }

        Map<String, String> agentAttributes = new HashMap<>();

        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "ami"), ami -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_AMI, ami));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "asg"), asg -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ASG, asg));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "cluster"), cluster -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_CLUSTER, cluster));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "id"), id -> {
            agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, id);
            agentAttributes.put("agent.id", id);
        });
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "itype"), itype -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ITYPE, itype));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "region"), region -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_REGION, region));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "res"), res -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_RES, res));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "stack"), stack -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_STACK, stack));
        acceptNotNull(annotations.get(NODE_ANNOTATION_PREFIX + "zone"), zone -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, zone));

        acceptNotNull(node.getMetadata().getName(), nodeName -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_KUBE_NODE_NAME, nodeName));

        String nodeIpAddress = KubeUtil.getNodeIpV4Address(node);
        agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, nodeIpAddress);
        agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST_IP, nodeIpAddress);

        return task.toBuilder()
                .withTaskContext(CollectionsExt.merge(task.getTaskContext(), agentAttributes))
                .build();
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
                // It must be the case where the container setup failed.
                return fillInMissingStatesForContainerSetupFailure(pod, task);
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

    /**
     * In case container could not be started, we do not have the container start time, only the finished time.
     * The {@link V1PodStatus#getPhase()} is failed, and the {@link V1PodStatus#getMessage()} contains details on
     * the nature of failure. There should be no launched state, so we add it to mark the container start attempt.
     */
    private Task fillInMissingStatesForContainerSetupFailure(V1Pod pod, Task task) {
        // Sanity check. Should never be true.
        if (JobFunctions.findTaskStatus(task, TaskState.Launched).isPresent()) {
            return task;
        }

        long startAtTimestamp;
        V1ContainerState containerState = KubeUtil.findContainerState(pod).orElse(null);
        if (containerState != null && containerState.getTerminated() != null && containerState.getTerminated().getFinishedAt() != null) {
            startAtTimestamp = containerState.getTerminated().getFinishedAt().getMillis();
        } else {
            startAtTimestamp = task.getStatus().getTimestamp();
        }

        List<TaskStatus> newStatusHistory = new ArrayList<>(task.getStatusHistory());
        newStatusHistory.add(TaskStatus.newBuilder()
                .withState(TaskState.Launched)
                .withReasonCode(TaskStatus.REASON_STATE_MISSING)
                .withReasonMessage("Filled in")
                .withTimestamp(startAtTimestamp)
                .build()
        );
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

    private Task addMissingAttributes(V1Pod pod, Task updatedTask) {
        Map<String, String> taskContext = updatedTask.getTaskContext();

        Optional<String> updatedCpus = getPodAnnotation(pod, KubeConstants.OPPORTUNISTIC_CPU_COUNT)
                .filter(cpus -> !cpus.equals(taskContext.get(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT)));

        Optional<String> updatedAllocation = getPodAnnotation(pod, KubeConstants.OPPORTUNISTIC_ID)
                .filter(id -> !id.equals(taskContext.get(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION)));

        if (!updatedCpus.isPresent() && !updatedAllocation.isPresent()) {
            return updatedTask; // no updates, nothing to do
        }

        Task.TaskBuilder<?, ?> builder = updatedTask.toBuilder();
        updatedCpus.ifPresent(cpus -> builder.addToTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, cpus));
        updatedAllocation.ifPresent(id -> builder.addToTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, id));
        return builder.build();
    }

    private Optional<String> getPodAnnotation(V1Pod pod, String key) {
        return Optional.ofNullable(pod.getMetadata())
                .flatMap(meta -> Optional.ofNullable(meta.getAnnotations()))
                .flatMap(annotations -> Optional.ofNullable(annotations.get(key)));
    }

}
