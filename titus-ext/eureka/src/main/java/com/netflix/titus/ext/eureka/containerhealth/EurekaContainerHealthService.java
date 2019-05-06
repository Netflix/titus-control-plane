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

package com.netflix.titus.ext.eureka.containerhealth;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthUpdateEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.ReactorRetriers;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Singleton
public class EurekaContainerHealthService implements ContainerHealthService {

    private static final Logger logger = LoggerFactory.getLogger(EurekaContainerHealthService.class);

    public static final String NAME = "eureka";

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(5);

    private final ReadOnlyJobOperations jobOperations;
    private final EurekaClient eurekaClient;
    private final TitusRuntime titusRuntime;

    private final Flux<ContainerHealthEvent> healthStatuses;
    private Disposable eventLoggerDisposable;

    @Inject
    public EurekaContainerHealthService(ReadOnlyJobOperations jobOperations, EurekaClient eurekaClient, TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.eurekaClient = eurekaClient;
        this.titusRuntime = titusRuntime;

        Flux<EurekaEvent> eurekaCallbacks = ReactorExt.fromListener(
                EurekaEventListener.class,
                eurekaClient::registerEventListener,
                eurekaClient::unregisterEventListener
        );

        this.healthStatuses = Flux.defer(() -> {
            ConcurrentMap<String, ContainerHealthEvent> current = new ConcurrentHashMap<>();
            return Flux.merge(eurekaCallbacks, ReactorExt.toFlux(jobOperations.observeJobs()))
                    .flatMap(event -> handleJobManagerOrEurekaStatusUpdate(event, current));
        }).share().compose(ReactorExt.badSubscriberHandler(logger));

    }

    @PostConstruct
    public void start() {
        this.eventLoggerDisposable = healthStatuses
                .retryWhen(ReactorRetriers.instrumentedRetryer("EurekaContainerHealthServiceEventLogger", RETRY_INTERVAL, logger))
                .subscribeOn(Schedulers.parallel())
                .subscribe(
                        event -> logger.info("Eureka health status update: {}", event),
                        e -> logger.error("Unexpected error"),
                        () -> logger.info("Eureka health event logger terminated")
                );
    }

    @PreDestroy
    public void shutdown() {
        ReactorExt.safeDispose(eventLoggerDisposable);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return jobOperations.findTaskById(taskId).map(jobAndTaskPair -> buildHealthStatus(jobAndTaskPair.getLeft(), jobAndTaskPair.getRight()));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        Preconditions.checkArgument(!snapshot, "Snapshot state is generated by AggregatingContainerHealthService");
        return healthStatuses;
    }

    private ContainerHealthStatus buildHealthStatus(Job<?> job, Task task) {
        return ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withTimestamp(titusRuntime.getClock().wallTime())
                .withState(takeStateOf(job, task))
                .withReason(takeStateReasonOf(job, task))
                .build();
    }

    private ContainerHealthStatus buildHealthStatus(Task task, ContainerHealthState state, String reason) {
        return ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withTimestamp(titusRuntime.getClock().wallTime())
                .withState(state)
                .withReason(reason)
                .build();
    }

    private ContainerHealthState takeStateOf(Job<?> job, Task task) {
        // If it is finished, ignore Eureka status
        if (task.getStatus().getState() == TaskState.Finished) {
            return ContainerHealthState.Terminated;
        }

        List<InstanceInfo> instances = eurekaClient.getInstancesById(task.getId());

        // If a job is disabled, the real Eureka state is hidden. If the container is not registered with Eureka in
        // the disabled job, we also do not put any constraints here. In both cases we report it is healthy.
        if (JobFunctions.isDisabled(job)) {
            return ContainerHealthState.Healthy;
        }

        if (CollectionsExt.isNullOrEmpty(instances)) {
            return ContainerHealthState.Unknown;
        }
        InstanceInfo instance = instances.get(0);

        return instance.getStatus() == InstanceInfo.InstanceStatus.UP
                ? ContainerHealthState.Healthy
                : ContainerHealthState.Unhealthy;
    }

    private String takeStateReasonOf(Job<?> job, Task task) {
        List<InstanceInfo> instances = eurekaClient.getInstancesById(task.getId());

        if (CollectionsExt.isNullOrEmpty(instances)) {
            return JobFunctions.isDisabled(job) ? "not registered, and job disabled" : "not registered";
        }

        // If it is finished, ignore Eureka status
        if (task.getStatus().getState() == TaskState.Finished) {
            return "terminated";
        }

        return instances.get(0).getStatus().name();
    }

    private Flux<ContainerHealthEvent> handleJobManagerOrEurekaStatusUpdate(Object event, ConcurrentMap<String, ContainerHealthEvent> state) {
        if (event instanceof JobManagerEvent) {
            return handleJobManagerEvent((JobManagerEvent) event, state);
        }
        if (event instanceof EurekaEvent) {
            return handleEurekaEvent((EurekaEvent) event, state);
        }
        return Flux.empty();
    }

    private Flux<ContainerHealthEvent> handleJobManagerEvent(JobManagerEvent event, ConcurrentMap<String, ContainerHealthEvent> state) {
        if (event instanceof JobUpdateEvent) {
            JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
            return jobUpdateEvent.getPrevious()
                    .map(previous -> handleJobEnabledStatusUpdate(jobUpdateEvent.getCurrent(), previous, state))
                    .orElse(Flux.empty());
        } else if (event instanceof TaskUpdateEvent) {
            TaskUpdateEvent taskEvent = (TaskUpdateEvent) event;
            return handleTaskStateUpdate(taskEvent.getCurrentJob(), taskEvent.getCurrentTask(), state).map(Flux::just).orElse(Flux.empty());
        }
        return Flux.empty();
    }

    private Flux<ContainerHealthEvent> handleEurekaEvent(EurekaEvent event, ConcurrentMap<String, ContainerHealthEvent> state) {
        if (!(event instanceof CacheRefreshedEvent)) {
            return Flux.empty();
        }

        List<Pair<Job, List<Task>>> allJobsAndTasks = jobOperations.getJobsAndTasks();
        List<Task> allTasks = new ArrayList<>();
        List<ContainerHealthEvent> events = new ArrayList<>();

        allJobsAndTasks.forEach(jobAndTasks -> {
            jobAndTasks.getRight().forEach(task -> {
                handleTaskStateUpdate(jobAndTasks.getLeft(), task, state).ifPresent(events::add);
                allTasks.add(task);
            });
        });

        // Cleanup, in case we have stale entries.
        Set<String> unknownTaskIds = CollectionsExt.copyAndRemove(state.keySet(), allTasks.stream().map(Task::getId).collect(Collectors.toSet()));
        unknownTaskIds.forEach(taskId -> {
            state.remove(taskId);

            // Assume the task was terminated.
            ContainerHealthStatus terminatedStatus = ContainerHealthStatus.newBuilder()
                    .withTaskId(taskId)
                    .withTimestamp(titusRuntime.getClock().wallTime())
                    .withState(ContainerHealthState.Terminated)
                    .withReason("terminated")
                    .build();

            events.add(ContainerHealthUpdateEvent.healthChanged(terminatedStatus));
        });

        return Flux.fromIterable(events);
    }

    private Flux<ContainerHealthEvent> handleJobEnabledStatusUpdate(Job current, Job previous, ConcurrentMap<String, ContainerHealthEvent> state) {

        if (!JobFunctions.isServiceJob(current)) {
            return Flux.empty();
        }

        // Examine if a job's 'enabled' status was changed.
        boolean isCurrentDisabled = JobFunctions.isDisabled(current);
        if (isCurrentDisabled == JobFunctions.isDisabled(previous)) {
            return Flux.empty();
        }

        List<Task> tasks = jobOperations.getTasks(current.getId());
        List<ContainerHealthEvent> events = new ArrayList<>();
        tasks.forEach(task -> handleTaskStateUpdate(current, task, state).ifPresent(events::add));

        return Flux.fromIterable(events);
    }

    private Optional<ContainerHealthEvent> handleTaskStateUpdate(Job<?> job, Task task, ConcurrentMap<String, ContainerHealthEvent> state) {
        ContainerHealthUpdateEvent lastEvent = (ContainerHealthUpdateEvent) state.get(task.getId());
        if (lastEvent == null) {
            return Optional.of(recordNewState(state, task, ContainerHealthEvent.healthChanged(buildHealthStatus(job, task))));
        }

        ContainerHealthState newTaskState = takeStateOf(job, task);
        String newReason = takeStateReasonOf(job, task);
        if (lastEvent.getContainerHealthStatus().getState() == newTaskState && lastEvent.getContainerHealthStatus().getReason().equals(newReason)) {
            return Optional.empty();
        }
        return Optional.of(recordNewState(state, task, ContainerHealthEvent.healthChanged(buildHealthStatus(task, newTaskState, newReason))));
    }

    private ContainerHealthUpdateEvent recordNewState(ConcurrentMap<String, ContainerHealthEvent> state, Task task, ContainerHealthUpdateEvent newEvent) {
        if (task.getStatus().getState() != TaskState.Finished) {
            state.put(task.getId(), newEvent);
        } else {
            state.remove(task.getId());
        }
        return newEvent;
    }
}
