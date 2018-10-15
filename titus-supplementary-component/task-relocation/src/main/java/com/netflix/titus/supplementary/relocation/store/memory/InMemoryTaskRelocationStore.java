package com.netflix.titus.supplementary.relocation.store.memory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import reactor.core.publisher.Mono;

@Singleton
public class InMemoryTaskRelocationStore implements TaskRelocationStore {

    private final ConcurrentMap<String, TaskRelocationPlan> taskRelocationPlanByTaskId = new ConcurrentHashMap<>();

    @Override
    public Mono<Map<String, Optional<Throwable>>> createOrUpdateTaskRelocationPlans(List<TaskRelocationPlan> taskRelocationPlans) {
        return Mono.defer(() -> {
            Map<String, Optional<Throwable>> result = new HashMap<>();
            taskRelocationPlans.forEach(status -> {
                        taskRelocationPlanByTaskId.put(status.getTaskId(), status);
                        result.put(status.getTaskId(), Optional.empty());
                    }
            );
            return Mono.just(result);
        });
    }

    @Override
    public Mono<Map<String, TaskRelocationPlan>> getAllTaskRelocationPlans() {
        return Mono.defer(() -> Mono.just(Collections.unmodifiableMap(taskRelocationPlanByTaskId)));
    }

    @Override
    public Mono<Map<String, Optional<Throwable>>> removeTaskRelocationPlans(Set<String> toRemove) {
        return Mono.defer(() -> {
            taskRelocationPlanByTaskId.keySet().removeAll(toRemove);
            Map<String, Optional<Throwable>> result = toRemove.stream().collect(Collectors.toMap(tid -> tid, tid -> Optional.empty()));
            return Mono.just(result);
        });
    }
}
