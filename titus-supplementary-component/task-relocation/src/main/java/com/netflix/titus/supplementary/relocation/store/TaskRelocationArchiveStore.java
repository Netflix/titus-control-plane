package com.netflix.titus.supplementary.relocation.store;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import reactor.core.publisher.Mono;

public interface TaskRelocationArchiveStore {

    /**
     * Creates or updates a record in the database.
     */
    Mono<Map<String, Optional<Throwable>>> createTaskRelocationStatuses(List<TaskRelocationStatus> taskRelocationStatuses);
}
