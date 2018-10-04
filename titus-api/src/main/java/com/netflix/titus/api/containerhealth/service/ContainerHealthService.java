package com.netflix.titus.api.containerhealth.service;

import java.util.Optional;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import reactor.core.publisher.Flux;

/**
 * This service provides information about the application level health status of a process running in a container.
 */
public interface ContainerHealthService {

    /**
     * Container health provider name.
     */
    String getName();

    default ContainerHealthStatus getHealthStatus(String taskId) {
        return findHealthStatus(taskId).orElseThrow(() -> JobManagerException.taskNotFound(taskId));
    }

    /**
     * Returns task's status, if the task with the given id is known or {@link Optional#empty()} otherwise.
     * If a task is in the {@link TaskState#Finished} state, its status will be returned as long as it is kept
     * in memory (its job is active, and it is not archived yet).
     */
    Optional<ContainerHealthStatus> findHealthStatus(String taskId);

    /**
     * Event stream which emits container health change notifications.
     *
     * @param snapshot if set to true, all known health states are emitted first, followed by the snapshot marker.
     */
    Flux<ContainerHealthEvent> events(boolean snapshot);
}
