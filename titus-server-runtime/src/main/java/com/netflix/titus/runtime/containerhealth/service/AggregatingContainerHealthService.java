package com.netflix.titus.runtime.containerhealth.service;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.containerhealth.model.ContainerHealthFunctions;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;

@Singleton
public class AggregatingContainerHealthService implements ContainerHealthService {

    public static final String NAME = "aggregating";

    private final Map<String, ContainerHealthService> healthServices;
    private final ReadOnlyJobOperations jobOperations;

    @Inject
    public AggregatingContainerHealthService(Set<ContainerHealthService> healthServices,
                                             ReadOnlyJobOperations jobOperations) {
        this.healthServices = healthServices.stream().collect(Collectors.toMap(ContainerHealthService::getName, Function.identity()));
        this.jobOperations = jobOperations;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return findJob(taskId).flatMap(job -> process(job, taskId));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return Flux.error(new RuntimeException("Not implemented yet"));
    }

    private Optional<Job<?>> findJob(String taskId) {
        return jobOperations.findTaskById(taskId).map(Pair::getLeft);
    }

    private Optional<ContainerHealthStatus> process(Job<?> job, String taskId) {
        // TODO Read it from the job descriptor
        Set<String> enabledServices = Collections.emptySet();

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
