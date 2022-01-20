/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.watcher;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.kubernetes.ContainerResultCodeResolver;
import com.netflix.titus.master.kubernetes.PodToTaskMapper;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * A helper service that periodically checks job service task states and its corresponding pods.
 * In perfect scenario where the data is not lost, and there are no processing latencies of Kube informer events, the
 * states should be very close by. This services measures how much is that true.
 */
@Singleton
public class KubeAndJobServiceSyncStatusWatcher {

    private static final Logger logger = LoggerFactory.getLogger(KubeAndJobServiceSyncStatusWatcher.class);

    private final StdKubeApiFacade kubeApiFacade;
    private final ReadOnlyJobOperations jobService;
    private final ContainerResultCodeResolver containerResultCodeResolver;
    private final TitusRuntime titusRuntime;

    private ScheduleReference schedulerRef;

    private final ConcurrentMap<String, TaskHolder> capturedState = new ConcurrentHashMap<>();

    @Inject
    public KubeAndJobServiceSyncStatusWatcher(StdKubeApiFacade kubeApiFacade,
                                              ReadOnlyJobOperations jobService,
                                              ContainerResultCodeResolver containerResultCodeResolver,
                                              TitusRuntime titusRuntime) {
        this.kubeApiFacade = kubeApiFacade;
        this.jobService = jobService;
        this.containerResultCodeResolver = containerResultCodeResolver;
        this.titusRuntime = titusRuntime;
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        try {
            kubeApiFacade.getPodInformer().addEventHandler(new ResourceEventHandler<V1Pod>() {
                @Override
                public void onAdd(V1Pod obj) {
                    capturedState.put(obj.getMetadata().getName(), new TaskHolder(obj, false));
                }

                @Override
                public void onUpdate(V1Pod oldObj, V1Pod newObj) {
                    capturedState.put(newObj.getMetadata().getName(), new TaskHolder(newObj, false));
                }

                @Override
                public void onDelete(V1Pod obj, boolean deletedFinalStateUnknown) {
                    capturedState.put(obj.getMetadata().getName(), new TaskHolder(obj, true));
                }
            });

            ScheduleDescriptor scheduleDescriptor = ScheduleDescriptor.newBuilder()
                    .withName(KubeAndJobServiceSyncStatusWatcher.class.getSimpleName())
                    .withDescription("Compare Kube pod state with Titus job service")
                    .withInitialDelay(Duration.ofSeconds(60))
                    .withInterval(Duration.ofSeconds(10))
                    .withTimeout(Duration.ofSeconds((60)))
                    .build();

            this.schedulerRef = titusRuntime.getLocalScheduler().schedule(
                    scheduleDescriptor,
                    this::process,
                    ExecutorsExt.namedSingleThreadExecutor(KubeAndJobServiceSyncStatusWatcher.class.getSimpleName())
            );
        } catch (Exception e) {
            return Observable.error(e);
        }
        return Observable.empty();
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

    private void process(ExecutionContext context) {
        if (!kubeApiFacade.getPodInformer().hasSynced()) {
            logger.info("Not synced yet");
            return;
        }

        try {
            if (!jobService.getJobsAndTasks().isEmpty()) {
                // Remove tasks not found in job service
                Set<String> taskIds = jobService.getJobsAndTasks().stream()
                        .flatMap(jobAndTasks -> jobAndTasks.getRight().stream())
                        .map(Task::getId)
                        .collect(Collectors.toSet());
                capturedState.keySet().retainAll(taskIds);

                // Update job service task state
                jobService.getJobsAndTasks().forEach(jobAndTasks ->
                        jobAndTasks.getRight().forEach(task -> {
                            TaskHolder taskHolder = capturedState.get(task.getId());
                            if (taskHolder == null || taskHolder.getPod() == null) {
                                capturedState.put(task.getId(), new TaskHolder(task));
                            } else {
                                capturedState.put(task.getId(), new TaskHolder(taskHolder.getPod(), taskHolder.isPodDeleted()));
                            }
                        })
                );
            }

            logger.info("Captured state size: {}", (long) capturedState.values().size());
            Map<String, Integer> stateCounts = new HashMap<>();
            capturedState.forEach((id, h) -> {
                String state = h.getSyncState();
                stateCounts.put(state, stateCounts.getOrDefault(state, 0) + 1);
            });
            stateCounts.forEach((state, count) -> logger.info("{}: {}", state, count));
        } catch (Exception e) {
            logger.error("Processing error", e);
        }
    }

    class TaskHolder {
        private final Task task;
        private final V1Pod pod;
        private final boolean podDeleted;
        private final long timestamp;
        private final String syncState;

        public TaskHolder(V1Pod pod, boolean podDeleted) {
            this.podDeleted = podDeleted;
            String id = pod.getMetadata().getName();
            Pair<Job<?>, Task> jobAndTask = jobService.findTaskById(id).orElse(null);
            if (jobAndTask == null) {
                this.task = null;
                BatchJobTask fakeTask = BatchJobTask.newBuilder()
                        .withId(pod.getMetadata().getName())
                        .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                        .build();
                Optional<TaskStatus> taskStatusInKubeOpt = extractedTaskStatus(fakeTask, pod, podDeleted);
                this.syncState = taskStatusInKubeOpt
                        .map(taskStatusInKube -> "noTask:" + taskStatusInKube.getState().name())
                        .orElse("noTask:unknown");
            } else {
                this.task = jobAndTask.getRight();
                Optional<TaskStatus> taskStatusInKubeOpt = extractedTaskStatus(task, pod, podDeleted);
                this.syncState = taskStatusInKubeOpt
                        .map(taskStatusInKube -> {
                            if (task.getStatus().getState() == taskStatusInKube.getState()) {
                                return "synced:" + taskStatusInKube.getState().name();
                            } else {
                                return "notSynced:" + taskStatusInKube.getState().name() + "!=" + task.getStatus().getState();
                            }
                        })
                        .orElseGet(() -> "notSynced:unknown!=" + task.getStatus().getState());
            }
            this.pod = pod;
            this.timestamp = System.currentTimeMillis();
        }

        public TaskHolder(Task task) {
            this.task = task;
            this.pod = null;
            this.podDeleted = TaskState.isTerminalState(task.getStatus().getState());
            this.timestamp = System.currentTimeMillis();
            this.syncState = "noPod:" + task.getStatus().getState();
        }

        public Task getTask() {
            return task;
        }

        public V1Pod getPod() {
            return pod;
        }

        public boolean isPodDeleted() {
            return podDeleted;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getSyncState() {
            return syncState;
        }

        private Optional<TaskStatus> extractedTaskStatus(Task task, V1Pod obj, boolean podDeleted) {
            PodToTaskMapper mapper = new PodToTaskMapper(new PodWrapper(obj), Optional.empty(), task, podDeleted, containerResultCodeResolver, titusRuntime);
            Either<TaskStatus, String> either = mapper.getNewTaskStatus();
            if (either.hasValue()) {
                return Optional.of(either.getValue());
            }
            return Optional.empty();
        }
    }
}
