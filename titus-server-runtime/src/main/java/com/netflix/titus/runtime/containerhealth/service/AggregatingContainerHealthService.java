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
                }).share().compose(ReactorExt.badSubscriberHandler(logger));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return jobOperations.findTaskById(taskId).flatMap(jobTaskPair -> takeStatusOf(jobTaskPair.getLeft(), jobTaskPair.getRight()));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return snapshot
                ? healthStatuses.compose(ReactorExt.head(() -> Collections.singletonList(buildCurrentSnapshot())))
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
                    snapshot.add(takeStatusOf(job, task).orElseGet(() -> ContainerHealthStatus.unknown(task.getId(), clock.wallTime())));
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
        return takeStatusOf(job, task)
                .map(newStatus -> {

                    ContainerHealthState previousState = emittedStates.get(task.getId());
                    ContainerHealthState newState = newStatus.getState();

                    if (newState == previousState) {
                        return Flux.<ContainerHealthEvent>empty();
                    }

                    if (newState == ContainerHealthState.Terminated) {
                        emittedStates.remove(task.getId());
                    } else {
                        emittedStates.put(task.getId(), newState);
                    }
                    return Flux.<ContainerHealthEvent>just(ContainerHealthEvent.healthChanged(newStatus));
                })
                .orElse(Flux.empty());
    }

    private Flux<ContainerHealthEvent> handleContainerHealthEventForUnknownTask(String taskId, ContainerHealthEvent event, ConcurrentMap<String, ContainerHealthState> emittedStates) {
        logger.info("Received health update event for an unknown task: taskId={}, event={}", taskId, event);
        emittedStates.remove(taskId);
        return Flux.empty();
    }

    private Optional<ContainerHealthStatus> takeStatusOf(Job<?> job, Task task) {
        Set<String> healthProviders = job.getJobDescriptor().getDisruptionBudget().getContainerHealthProviders().stream()
                .map(ContainerHealthProvider::getName)
                .filter(healthServices::containsKey)
                .collect(Collectors.toSet());

        return healthProviders.isEmpty()
                ? taskStatusOfTaskWithNoHealthProviders(task)
                : takeStatusOfTaskWithHealthProviders(task, healthProviders);
    }

    private Optional<ContainerHealthStatus> taskStatusOfTaskWithNoHealthProviders(Task task) {
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
        return Optional.of(ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withState(healthState)
                .withTimestamp(clock.wallTime())
                .build()
        );
    }

    private Optional<ContainerHealthStatus> takeStatusOfTaskWithHealthProviders(Task task, Set<String> enabledServices) {
        ContainerHealthStatus current = null;
        for (String name : enabledServices) {
            ContainerHealthService healthService = healthServices.get(name);
            if (healthService != null) {
                Optional<ContainerHealthStatus> result = healthService.findHealthStatus(task.getId());
                if (result.isPresent()) {
                    current = current == null ? result.get() : ContainerHealthFunctions.merge(current, result.get());
                }
            }
        }

        return current == null
                ? Optional.of(ContainerHealthStatus.unknown(task.getId(), clock.wallTime()))
                : Optional.of(current);
    }
}
