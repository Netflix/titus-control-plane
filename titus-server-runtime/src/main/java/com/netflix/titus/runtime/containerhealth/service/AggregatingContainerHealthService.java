package com.netflix.titus.runtime.containerhealth.service;

import java.time.Duration;
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
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static com.netflix.titus.common.util.CollectionsExt.transformSet;

@Singleton
public class AggregatingContainerHealthService implements ContainerHealthService {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingContainerHealthService.class);

    public static final String NAME = "aggregating";

    private static final long RETRY_INTERVAL_MS = 5_000;

    private final Map<String, ContainerHealthService> healthServices;
    private final ReadOnlyJobOperations jobOperations;
    private final Set<String> defaultProviders;
    private final TitusRuntime titusRuntime;
    private final Clock clock;

    private final Flux<ContainerHealthEvent> healthStatuses;

    @Inject
    public AggregatingContainerHealthService(Set<ContainerHealthService> healthServices,
                                             ReadOnlyJobOperations jobOperations,
                                             Set<String> defaultProviders,
                                             TitusRuntime titusRuntime) {
        this.healthServices = healthServices.stream().collect(Collectors.toMap(ContainerHealthService::getName, Function.identity()));
        this.jobOperations = jobOperations;
        this.defaultProviders = defaultProviders;
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();

        this.healthStatuses = Flux
                .defer(() -> {
                    ConcurrentMap<String, ContainerHealthState> emittedStates = new ConcurrentHashMap<>();

                    return Flux
                            .merge(transformSet(healthServices, this::buildRetryableStream))
                            .flatMap(event -> handleContainerHealthUpdateEvent(event, emittedStates));
                }).share().compose(ReactorExt.badSubscriberHandler(logger));
    }

    private Flux<ContainerHealthEvent> buildRetryableStream(ContainerHealthService h) {
        return h.events(false)
                .retryWhen(errors -> errors.flatMap(e -> {
                    logger.warn(
                            "Downstream container health provider terminated with an error, retrying in {}ms: provider={}, error={}",
                            h.getName(), e.getMessage(), RETRY_INTERVAL_MS
                    );
                    logger.debug("Stack trace", e);
                    return Flux.interval(Duration.ofMillis(RETRY_INTERVAL_MS)).take(1);
                }));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return findJob(taskId).flatMap(job -> takeStatusOf(job, taskId));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return snapshot
                ? healthStatuses.compose(ReactorExt.head(() -> Collections.singletonList(buildCurrentSnapshot())))
                : healthStatuses;
    }

    private Optional<Job<?>> findJob(String taskId) {
        return jobOperations.findTaskById(taskId).map(Pair::getLeft);
    }

    private ContainerHealthSnapshotEvent buildCurrentSnapshot() {
        List<ContainerHealthStatus> snapshot = new ArrayList<>();
        jobOperations.getJobsAndTasks().forEach(p -> {
            Job job = p.getLeft();
            p.getRight().forEach(task -> {
                if (task.getStatus().getState() == TaskState.Finished) {
                    snapshot.add(ContainerHealthStatus.terminated(task.getId(), titusRuntime.getClock().wallTime()));
                } else {
                    snapshot.add(takeStatusOf(job, task.getId()).orElseGet(() -> ContainerHealthStatus.unknown(task.getId(), clock.wallTime())));
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
        return takeStatusOf(job, task.getId())
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

    private Optional<ContainerHealthStatus> takeStatusOf(Job<?> job, String taskId) {
        // TODO Read it from the job descriptor
        Set<String> enabledServices = defaultProviders;

        ContainerHealthStatus current = null;
        for (String name : enabledServices) {
            ContainerHealthService healthService = healthServices.get(name);
            if (healthService != null) {
                Optional<ContainerHealthStatus> result = healthService.findHealthStatus(taskId);
                if (result.isPresent()) {
                    current = current == null ? result.get() : ContainerHealthFunctions.merge(current, result.get());
                }
            }
        }
        return Optional.ofNullable(current);
    }
}
