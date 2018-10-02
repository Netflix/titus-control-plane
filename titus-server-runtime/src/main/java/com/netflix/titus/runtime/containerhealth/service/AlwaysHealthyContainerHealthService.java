package com.netflix.titus.runtime.containerhealth.service;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.publisher.Flux;

@Singleton
public class AlwaysHealthyContainerHealthService implements ContainerHealthService {

    private static final String NAME = "alwaysHealthy";

    private final V3JobOperations jobOperations;

    @Inject
    public AlwaysHealthyContainerHealthService(V3JobOperations jobOperations) {
        this.jobOperations = jobOperations;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return jobOperations.findTaskById(taskId).map(jobAndTaskPair -> buildHealthStatus(jobAndTaskPair.getRight()));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return ReactorExt.toFlux(jobOperations.observeJobs()).flatMap(event -> {
            if (event instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                Task task = taskUpdateEvent.getCurrentTask();
                return Flux.just(ContainerHealthEvent.healthChanged(buildHealthStatus(task)));
            }
            return Flux.empty();
        });
    }

    private ContainerHealthStatus buildHealthStatus(Task task) {
        return ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withState(task.getStatus().getState() == TaskState.Finished ? ContainerHealthState.Terminated : ContainerHealthState.Healthy)
                .withTimestamp(System.currentTimeMillis())
                .build();
    }
}
