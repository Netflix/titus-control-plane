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
