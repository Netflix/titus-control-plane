package com.netflix.titus.supplementary.relocation.store.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationArchiveStore;
import reactor.core.publisher.Mono;

@Singleton
public class InMemoryTaskRelocationArchiveStore implements TaskRelocationArchiveStore {

    private final ConcurrentMap<String, List<TaskRelocationStatus>> taskRelocationStatusesByTaskId = new ConcurrentHashMap<>();

    @Override
    public Mono<Map<String, Optional<Throwable>>> createTaskRelocationStatuses(List<TaskRelocationStatus> taskRelocationStatuses) {
        return Mono.defer(() -> {
            Map<String, Optional<Throwable>> result = new HashMap<>();
            synchronized (taskRelocationStatusesByTaskId) {
                taskRelocationStatuses.forEach(status -> {
                            taskRelocationStatusesByTaskId.computeIfAbsent(
                                    status.getTaskId(),
                                    tid -> new ArrayList<>()
                            ).add(status);
                            result.put(status.getTaskId(), Optional.empty());
                        }
                );
            }
            return Mono.just(result);
        });
    }
}
