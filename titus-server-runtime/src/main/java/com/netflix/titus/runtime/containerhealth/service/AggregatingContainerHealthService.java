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

package com.netflix.titus.runtime.containerhealth.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.containerhealth.model.ContainerHealthFunctions;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthSnapshotEvent;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthUpdateEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.ContainerHealthProvider;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static com.netflix.titus.common.util.CollectionsExt.transformSet;

@Singleton
public class AggregatingContainerHealthService implements ContainerHealthService {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingContainerHealthService.class);

    public static final String NAME = "aggregating";

    private final Map<String, ContainerHealthService> healthServices;
    private final ReadOnlyJobOperations jobOperations;
    private final TitusRuntime titusRuntime;
    private final Clock clock;

    private final Flux<ContainerHealthEvent> healthStatuses;

    @Inject
    public AggregatingContainerHealthService(Set<ContainerHealthService> healthServices,
                                             ReadOnlyJobOperations jobOperations,
                                             TitusRuntime titusRuntime) {
        logger.info("Registered container health services: {}", healthServices.stream().map(ContainerHealthService::getName).collect(Collectors.toList()));

        this.healthServices = healthServices.stream().collect(Collectors.toMap(ContainerHealthService::getName, Function.identity()));
        this.jobOperations = jobOperations;
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();

        this.healthStatuses = Flux
                .defer(() -> {
                    ConcurrentMap<String, ContainerHealthState> emittedStates = new ConcurrentHashMap<>();

                    return Flux
                            .merge(transformSet(healthServices, h -> h.events(false)))
                            .flatMap(event -> handleContainerHealthUpdateEvent(event, emittedStates));
                }).share().transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return jobOperations.findTaskById(taskId).map(jobTaskPair -> takeStatusOf(jobTaskPair.getLeft(), jobTaskPair.getRight()));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return snapshot
                ? healthStatuses.transformDeferred(ReactorExt.head(() -> Collections.singletonList(buildCurrentSnapshot())))
                : healthStatuses;
    }

    private ContainerHealthSnapshotEvent buildCurrentSnapshot() {
        List<ContainerHealthStatus> snapshot = new ArrayList<>();
        jobOperations.getJobsAndTasks().forEach(p -> {
            Job job = p.getLeft();
            p.getRight().forEach(task -> {
                if (task.getStatus().getState() == TaskState.Finished) {
                    snapshot.add(ContainerHealthStatus.terminated(task.getId(), titusRuntime.getClock().wallTime()));
                } else {
                    snapshot.add(takeStatusOf(job, task));
                }
            });
        });
        return new ContainerHealthSnapshotEvent(snapshot);
    }

    private Flux<ContainerHealthEvent> handleContainerHealthUpdateEvent(ContainerHealthEvent event, ConcurrentMap<String, ContainerHealthState> emittedStates) {
        if (!(event instanceof ContainerHealthUpdateEvent)) {
            return Flux.empty();
        }

        String taskId = ((ContainerHealthUpdateEvent) event).getContainerHealthStatus().getTaskId();

        return jobOperations.findTaskById(taskId)
                .map(pair -> handleNewState(pair.getLeft(), pair.getRight(), emittedStates))
                .orElseGet(() -> handleContainerHealthEventForUnknownTask(taskId, event, emittedStates));
    }

    private Flux<ContainerHealthEvent> handleNewState(Job<?> job, Task task, ConcurrentMap<String, ContainerHealthState> emittedStates) {
        ContainerHealthStatus newStatus = takeStatusOf(job, task);
        ContainerHealthState previousState = emittedStates.get(task.getId());
        ContainerHealthState newState = newStatus.getState();

        if (newState == previousState) {
            return Flux.empty();
        }

        if (newState == ContainerHealthState.Terminated) {
            emittedStates.remove(task.getId());
        } else {
            emittedStates.put(task.getId(), newState);
        }
        return Flux.just(ContainerHealthEvent.healthChanged(newStatus));
    }

    private Flux<ContainerHealthEvent> handleContainerHealthEventForUnknownTask(String taskId, ContainerHealthEvent event, ConcurrentMap<String, ContainerHealthState> emittedStates) {
        logger.info("Received health update event for an unknown task: taskId={}, event={}", taskId, event);
        emittedStates.remove(taskId);
        return Flux.empty();
    }

    private ContainerHealthStatus takeStatusOf(Job<?> job, Task task) {
        Set<String> healthProviders = job.getJobDescriptor().getDisruptionBudget().getContainerHealthProviders().stream()
                .map(ContainerHealthProvider::getName)
                .filter(healthServices::containsKey)
                .collect(Collectors.toSet());

        return healthProviders.isEmpty()
                ? taskStatusOfTaskWithNoHealthProviders(task)
                : takeStatusOfTaskWithHealthProviders(task, healthProviders);
    }

    private ContainerHealthStatus taskStatusOfTaskWithNoHealthProviders(Task task) {
        ContainerHealthState healthState;
        switch (task.getStatus().getState()) {
            case Accepted:
            case Launched:
            case StartInitiated:
            case KillInitiated:
                healthState = ContainerHealthState.Unhealthy;
                break;
            case Started:
                healthState = ContainerHealthState.Healthy;
                break;
            case Disconnected:
                healthState = ContainerHealthState.Unknown;
                break;
            case Finished:
                healthState = ContainerHealthState.Terminated;
                break;
            default:
                healthState = ContainerHealthState.Unknown;
        }
        return ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withState(healthState)
                .withReason(task.getStatus().getState() + " != Started")
                .withTimestamp(clock.wallTime())
                .build();
    }

    private ContainerHealthStatus takeStatusOfTaskWithHealthProviders(Task task, Set<String> enabledServices) {
        ContainerHealthStatus current = null;

        for (String name : enabledServices) {
            ContainerHealthService healthService = healthServices.get(name);
            ContainerHealthStatus newStatus;
            if (healthService != null) {
                newStatus = healthService
                        .findHealthStatus(task.getId())
                        .orElseGet(() -> ContainerHealthStatus.newBuilder()
                                .withTaskId(task.getId())
                                .withState(ContainerHealthState.Unknown)
                                .withReason("not known to: " + name)
                                .withTimestamp(clock.wallTime())
                                .build()
                        );
            } else {
                newStatus = ContainerHealthStatus.newBuilder()
                        .withTaskId(task.getId())
                        .withState(ContainerHealthState.Unknown)
                        .withReason("unknown container health provider set: " + name)
                        .withTimestamp(clock.wallTime())
                        .build();
            }
            current = current == null ? newStatus : ContainerHealthFunctions.merge(current, newStatus);
        }

        return current;
    }
}
