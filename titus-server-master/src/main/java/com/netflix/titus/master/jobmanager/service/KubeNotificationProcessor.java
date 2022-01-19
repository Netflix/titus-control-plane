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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Timer;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.kubernetes.ContainerResultCodeResolver;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.master.kubernetes.PodToTaskMapper;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.model.PodDeletedEvent;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import com.netflix.titus.master.kubernetes.client.model.PodNotFoundEvent;
import com.netflix.titus.master.kubernetes.client.model.PodUpdatedEvent;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.master.kubernetes.controller.KubeJobManagementReconciler;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import static com.netflix.titus.common.util.Evaluators.acceptNotNull;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.TITUS_NODE_DOMAIN;

/**
 * TODO Incorporate this into {@link DefaultV3JobOperations} once Fenzo is removed.
 */
@Singleton
public class KubeNotificationProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KubeNotificationProcessor.class);
    private static final CallMetadata KUBE_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Kube").build();

    private static final String METRICS_ROOT = MetricConstants.METRIC_JOB_MANAGER + "kubeNotificationProcessor.";

    private final DirectKubeApiServerIntegrator kubeApiServerIntegrator;
    private final KubeJobManagementReconciler kubeJobManagementReconciler;
    private final V3JobOperations v3JobOperations;
    private final ContainerResultCodeResolver containerResultCodeResolver;
    private final TitusRuntime titusRuntime;

    private final Timer metricsProcessed;
    private final Gauge metricsRunning;
    private final Gauge metricsLag;
    private final Counter metricsChangesApplied;
    private final Counter metricsNoChangesApplied;

    private ExecutorService notificationHandlerExecutor;
    private Scheduler scheduler;
    private Disposable subscription;

    @Inject
    public KubeNotificationProcessor(DirectKubeApiServerIntegrator kubeApiServerIntegrator,
                                     KubeJobManagementReconciler kubeJobManagementReconciler,
                                     V3JobOperations v3JobOperations,
                                     ContainerResultCodeResolver containerResultCodeResolver,
                                     TitusRuntime titusRuntime) {
        this.kubeApiServerIntegrator = kubeApiServerIntegrator;
        this.kubeJobManagementReconciler = kubeJobManagementReconciler;
        this.v3JobOperations = v3JobOperations;
        this.containerResultCodeResolver = containerResultCodeResolver;
        this.titusRuntime = titusRuntime;

        this.metricsProcessed = titusRuntime.getRegistry().timer(METRICS_ROOT + "processed");
        this.metricsRunning = titusRuntime.getRegistry().gauge(METRICS_ROOT + "running");
        this.metricsLag = titusRuntime.getRegistry().gauge(METRICS_ROOT + "lag");
        this.metricsChangesApplied = titusRuntime.getRegistry().counter(METRICS_ROOT + "changes", "changed", "true");
        this.metricsNoChangesApplied = titusRuntime.getRegistry().counter(METRICS_ROOT + "changes", "changed", "false");
    }

    @Activator
    public void enterActiveMode() {
        this.scheduler = initializeNotificationScheduler();
        AtomicLong pendingCounter = new AtomicLong();
        this.subscription = kubeApiServerIntegrator.events().mergeWith(kubeJobManagementReconciler.getPodEventSource())
                .subscribeOn(scheduler)
                .publishOn(scheduler)
                .doOnError(error -> logger.warn("Kube integration event stream terminated with an error (retrying soon)", error))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)))
                .subscribe(
                        event -> {
                            Stopwatch stopwatch = Stopwatch.createStarted();
                            pendingCounter.getAndIncrement();

                            metricsRunning.set(pendingCounter.get());
                            metricsLag.set(PodEvent.nextSequence() - event.getSequenceNumber());

                            logger.info("New event [pending={}, lag={}]: {}", pendingCounter.get(), PodEvent.nextSequence() - event.getSequenceNumber(), event);
                            processEvent(event)
                                    .doAfterTerminate(() -> {
                                        pendingCounter.decrementAndGet();
                                        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                        metricsProcessed.record(elapsed, TimeUnit.MILLISECONDS);
                                        metricsRunning.set(pendingCounter.get());
                                        logger.info("Event processed [pending={}]: event={}, elapsed={}", pendingCounter.get(), event, elapsed);
                                    })
                                    .subscribe(
                                            next -> {
                                                // nothing
                                            },
                                            error -> {
                                                logger.info("Kube notification event state update error: event={}, error={}", event, error.getMessage());
                                                logger.debug("Stack trace", error);
                                            },
                                            () -> {
                                                // nothing
                                            }
                                    );
                        },
                        e -> logger.error("Event stream terminated"),
                        () -> logger.info("Event stream completed")
                );
    }

    @VisibleForTesting
    protected Scheduler initializeNotificationScheduler() {
        this.notificationHandlerExecutor = ExecutorsExt.namedSingleThreadExecutor(KubeNotificationProcessor.class.getSimpleName());
        return Schedulers.fromExecutor(notificationHandlerExecutor);
    }

    private Mono<Void> processEvent(PodEvent event) {
        Pair<Job<?>, Task> jobAndTask = v3JobOperations.findTaskById(event.getTaskId()).orElse(null);
        if (jobAndTask == null) {
            logger.warn("Got Kube notification about unknown task: {}", event.getTaskId());
            return Mono.empty();
        }

        Task task = jobAndTask.getRight();
        if (event instanceof PodNotFoundEvent) {
            return handlePodNotFoundEvent((PodNotFoundEvent) event);
        }
        // We depend in this flatmap on the fact that the task update event is added by the source thread to
        // the job reconciler queue. This guarantees the right order of the execution.
        // TODO Implement flatMapWithSequentialSubscription operator
        return handlePodUpdatedEvent(event, jobAndTask.getLeft(), task);
    }

    public void shutdown() {
        ReactorExt.safeDispose(subscription);
        Evaluators.acceptNotNull(scheduler, Scheduler::dispose);
        Evaluators.acceptNotNull(notificationHandlerExecutor, ExecutorService::shutdown);
    }

    private Mono<Void> handlePodUpdatedEvent(PodEvent event, Job job, Task task) {
        // This is basic sanity check. If it fails, we have a major problem with pod state.
        if (event.getPod() == null || event.getPod().getStatus() == null || event.getPod().getStatus().getPhase() == null) {
            logger.warn("Pod notification with pod without status or phase set: taskId={}, pod={}", task.getId(), event.getPod());
            metricsNoChangesApplied.increment();
            return Mono.empty();
        }

        PodWrapper podWrapper = new PodWrapper(event.getPod());
        Optional<V1Node> node;
        if (event instanceof PodUpdatedEvent) {
            node = ((PodUpdatedEvent) event).getNode();
        } else if (event instanceof PodDeletedEvent) {
            node = ((PodDeletedEvent) event).getNode();
        } else {
            node = Optional.empty();
        }

        Either<TaskStatus, String> newTaskStatusOrError = new PodToTaskMapper(podWrapper, node, task,
                event instanceof PodDeletedEvent,
                containerResultCodeResolver,
                titusRuntime
        ).getNewTaskStatus();
        if (newTaskStatusOrError.hasError()) {
            logger.info(newTaskStatusOrError.getError());
            metricsNoChangesApplied.increment();
            return Mono.empty();
        }

        TaskStatus newTaskStatus = newTaskStatusOrError.getValue();
        if (TaskStatus.areEquivalent(task.getStatus(), newTaskStatus)) {
            logger.info("Pod change notification does not change task status: taskId={}, status={}, eventSequenceNumber={}", task.getId(), newTaskStatus, event.getSequenceNumber());
        } else {
            logger.info("Pod notification changes task status: taskId={}, fromStatus={}, toStatus={}, eventSequenceNumber={}", task.getId(),
                    task.getStatus(), newTaskStatus, event.getSequenceNumber());
        }

        Optional<TitusExecutorDetails> executorDetailsOpt = KubeUtil.getTitusExecutorDetails(event.getPod());

        // Check if the task is changed early before creating change action in the job service. If there is no material
        // change to the task, return immediately. If there are differences, we will check again in the change action
        // against most up to date task version.
        if (!updateTaskStatus(podWrapper, newTaskStatus, executorDetailsOpt, node, task, true).isPresent()) {
            return Mono.empty();
        }

        return ReactorExt.toMono(v3JobOperations.updateTask(
                task.getId(),
                current -> updateTaskStatus(podWrapper, newTaskStatus, executorDetailsOpt, node, current, false),
                V3JobOperations.Trigger.Kube,
                "Pod status updated from kubernetes node (k8phase='" + event.getPod().getStatus().getPhase() + "', taskState=" + task.getStatus().getState() + ")",
                KUBE_CALL_METADATA
        ));
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

                    metricsChangesApplied.increment();

                    return Optional.of(updatedTask);
                },
                V3JobOperations.Trigger.Kube,
                "Pod status updated from kubernetes node, it couldn't find the pod " + task.getId(),
                KUBE_CALL_METADATA
        ));
    }

    @VisibleForTesting
    Optional<Task> updateTaskStatus(PodWrapper podWrapper,
                                    TaskStatus newTaskStatus,
                                    Optional<TitusExecutorDetails> executorDetailsOpt,
                                    Optional<V1Node> node,
                                    Task currentTask,
                                    boolean precheck) {

        // This may happen as we build 'newTaskStatus' outside of the reconciler transaction. A real example:
        // 1. a job is terminated by a user
        // 2. reconciler moves the job's task to the 'KillInitiated' state, and calls KubeAPI.deleteNamespacedPod
        // 3. before `KubeAPI.deleteNamespacedPod` completes, KubeAPI sends pod notification triggered by that action
        // 4. pod notification handler receives the update. The latest committed task state is still 'Started'.
        // 5. a new job transaction is created to write pod update (taskState='Started'), and is put into the reconciler queue
        // 6. `KubeAPI.deleteNamespacedPod` completes, and the new task state 'KillInitiated' is written
        // 7. the pod transaction is taken off the queue and is executed, and writes new task state 'Started'
        // 8. in the next reconciliation loop a task is moved again to 'KillInitiated' state.
        if (TaskState.isBefore(newTaskStatus.getState(), currentTask.getStatus().getState())) {
            logger.info("[precheck={}] Ignoring an attempt to move the task state to the earlier one: taskId={}, attempt={}, current={}",
                    precheck, currentTask.getId(), newTaskStatus.getState(), currentTask.getStatus().getState()
            );
            metricsNoChangesApplied.increment();
            return Optional.empty();
        }

        Task updatedTask;
        if (TaskStatus.areEquivalent(currentTask.getStatus(), newTaskStatus)) {
            updatedTask = currentTask;
        } else {
            List<TaskStatus> newHistory = CollectionsExt.copyAndAdd(currentTask.getStatusHistory(), currentTask.getStatus());
            updatedTask = currentTask.toBuilder()
                    .withStatus(newTaskStatus)
                    .withStatusHistory(newHistory)
                    .build();
        }

        Task fixedTask = fillInMissingStates(podWrapper, updatedTask);
        Task taskWithExecutorData;
        if (executorDetailsOpt.isPresent()) {
            taskWithExecutorData = JobManagerUtil.attachTitusExecutorNetworkData(fixedTask, executorDetailsOpt);
        } else {
            taskWithExecutorData = JobManagerUtil.attachKubeletNetworkData(fixedTask, podWrapper);
        }
        Task taskWithNodeMetadata = node.map(n -> attachNodeMetadata(taskWithExecutorData, n)).orElse(taskWithExecutorData);

        Optional<String> difference = areTasksEquivalent(currentTask, taskWithNodeMetadata);
        if (!difference.isPresent()) {
            logger.debug("[precheck={}] Ignoring the pod event as the update results in the identical task object as the current one: taskId={}",
                    precheck, currentTask.getId());
            metricsNoChangesApplied.increment();
            return Optional.empty();
        }

        if (!precheck) {
            logger.info("[precheck={}] Tasks are different: difference='{}', current={}, updated={}",
                    precheck, difference.get(), currentTask, taskWithNodeMetadata);
            metricsChangesApplied.increment();
        }
        return Optional.of(taskWithNodeMetadata);
    }

    private static Task attachNodeMetadata(Task task, V1Node node) {
        Map<String, String> annotations = node.getMetadata().getAnnotations();
        if (CollectionsExt.isNullOrEmpty(annotations)) {
            return task;
        }

        Map<String, String> agentAttributes = new HashMap<>();

        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "ami"), ami -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_AMI, ami));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "asg"), asg -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ASG, asg));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "cluster"), cluster -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_CLUSTER, cluster));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "id"), id -> {
            agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, id);
            agentAttributes.put("agent.id", id);
        });
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "itype"), itype -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ITYPE, itype));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "region"), region -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_REGION, region));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "res"), res -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_RES, res));
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "stack"), stack -> {
            agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_STACK, stack);
            agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_STACK, stack);
        });
        acceptNotNull(annotations.get(TITUS_NODE_DOMAIN + "zone"), zone -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, zone));

        acceptNotNull(node.getMetadata().getName(), nodeName -> agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_KUBE_NODE_NAME, nodeName));

        String nodeIpAddress = KubeUtil.getNodeIpV4Address(node).orElse("UnknownIpAddress");
        agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, nodeIpAddress);
        agentAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST_IP, nodeIpAddress);

        return task.toBuilder()
                .withTaskContext(CollectionsExt.merge(task.getTaskContext(), agentAttributes))
                .build();
    }

    private static Task fillInMissingStates(PodWrapper podWrapper, Task task) {
        TaskState currentState = task.getStatus().getState();
        if (currentState != TaskState.Started && currentState != TaskState.Finished) {
            return task;
        }

        V1ContainerState containerState = podWrapper.findContainerState().orElse(null);
        if (containerState == null) {
            return task;
        }

        long startAtTimestamp;
        if (currentState == TaskState.Started) {
            if (containerState.getRunning() == null || containerState.getRunning().getStartedAt() == null) {
                return task;
            }
            startAtTimestamp = containerState.getRunning().getStartedAt().toInstant().toEpochMilli();
        } else { // TaskState.Finished
            if (containerState.getTerminated() == null || containerState.getTerminated().getStartedAt() == null) {
                // It must be the case where the container setup failed.
                return fillInMissingStatesForContainerSetupFailure(podWrapper, task);
            }
            startAtTimestamp = containerState.getTerminated().getStartedAt().toInstant().toEpochMilli();
        }

        TaskStatus.Builder statusTemplate = TaskStatus.newBuilder()
                .withReasonCode(TaskStatus.REASON_STATE_MISSING)
                .withReasonMessage("Filled in missing state update that was missed previously")
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
    private static Task fillInMissingStatesForContainerSetupFailure(PodWrapper podWrapper, Task task) {
        // Sanity check. Should never be true.
        if (JobFunctions.findTaskStatus(task, TaskState.Launched).isPresent()) {
            return task;
        }

        long startAtTimestamp = podWrapper.findFinishedAt().orElse(task.getStatus().getTimestamp());

        List<TaskStatus> newStatusHistory = new ArrayList<>(task.getStatusHistory());
        newStatusHistory.add(TaskStatus.newBuilder()
                .withState(TaskState.Launched)
                .withReasonCode(TaskStatus.REASON_STATE_MISSING)
                .withReasonMessage("Filled in missing state update that was missed previously due to container setup failure")
                .withTimestamp(startAtTimestamp)
                .build()
        );
        newStatusHistory.sort(Comparator.comparing(ExecutableStatus::getState));

        return task.toBuilder().withStatusHistory(newStatusHistory).build();
    }

    private static Optional<TaskStatus> addIfMissing(Task task, TaskState expectedState, TaskStatus.Builder statusTemplate) {
        Optional<TaskStatus> foundStatus = JobFunctions.findTaskStatus(task, expectedState);
        if (!foundStatus.isPresent()) {
            logger.debug("Adding missing task status: {} for task: {}", expectedState, task.getId());
            return Optional.of(statusTemplate.withState(expectedState).build());
        }
        return Optional.empty();
    }

    /**
     * 'updatedTask' is a modified copy of 'currentTask' (or exactly the same version of the object if nothing changed).
     */
    @VisibleForTesting
    static Optional<String> areTasksEquivalent(Task currentTask, Task updatedTask) {
        if (currentTask == updatedTask) {
            return Optional.empty();
        }
        if (!TaskStatus.areEquivalent(currentTask.getStatus(), updatedTask.getStatus())) {
            return Optional.of("different task status");
        }
        if (!currentTask.getAttributes().equals(updatedTask.getAttributes())) {
            return Optional.of("different task attributes");
        }
        if (!currentTask.getTaskContext().equals(updatedTask.getTaskContext())) {
            return Optional.of("different task context");
        }
        if (!currentTask.getTwoLevelResources().equals(updatedTask.getTwoLevelResources())) {
            return Optional.of("different task two level resources");
        }
        return Optional.empty();
    }
}
