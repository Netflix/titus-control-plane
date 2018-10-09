package com.netflix.titus.supplementary.relocation.store.memory;

import java.util.Map;
import javax.inject.Singleton;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import reactor.core.publisher.Mono;

@Singleton
public class InMemoryTaskRelocationStore implements TaskRelocationStore {
    @Override
    public Mono<Void> createOrUpdateTaskRelocationPlan(TaskRelocationPlan taskRelocationPlan) {
        return null;
    }

    @Override
    public Mono<Void> createTaskRelocationStatus(TaskRelocationStatus taskRelocationStatus) {
        return null;
    }

    @Override
    public Mono<Map<String, TaskRelocationPlan>> getAllTaskRelocationPlans() {
        return null;
    }

    @Override
    public Mono<Map<String, TaskRelocationStatus>> getAllTaskRelocationStatuses() {
        return null;
    }
}
