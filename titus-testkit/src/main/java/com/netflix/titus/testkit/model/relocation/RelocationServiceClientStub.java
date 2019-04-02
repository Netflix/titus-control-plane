/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.testkit.model.relocation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RelocationServiceClientStub implements RelocationServiceClient {

    private final Map<String, TaskRelocationPlan> taskRelocationPlans = new HashMap<>();
    private final DirectProcessor<TaskRelocationEvent> eventProcessor = DirectProcessor.create();

    @Override
    public Mono<Optional<TaskRelocationPlan>> findTaskRelocationPlan(String taskId) {
        return Mono.fromCallable(() -> Optional.ofNullable(taskRelocationPlans.get(taskId)));
    }

    @Override
    public Mono<List<TaskRelocationPlan>> findTaskRelocationPlans(Set<String> taskIds) {
        return Mono.fromCallable(() -> taskIds.stream()
                .filter(taskRelocationPlans::containsKey)
                .map(taskRelocationPlans::get)
                .collect(Collectors.toList())
        );
    }

    @Override
    public Flux<TaskRelocationEvent> events(TaskRelocationQuery query) {
        return eventProcessor
                .compose(ReactorExt.head(() -> {
                            List<TaskRelocationEvent> snapshot = taskRelocationPlans.values().stream()
                                    .map(TaskRelocationEvent::taskRelocationPlanUpdated)
                                    .collect(Collectors.toList());
                            snapshot.add(TaskRelocationEvent.newSnapshotEndEvent());
                            return snapshot;
                        }
                ));
    }

    public void addPlan(TaskRelocationPlan plan) {
        taskRelocationPlans.put(plan.getTaskId(), plan);
        eventProcessor.onNext(TaskRelocationEvent.taskRelocationPlanUpdated(plan));
    }

    public void removePlan(String taskId) {
        if (taskRelocationPlans.remove(taskId) != null) {
            eventProcessor.onNext(TaskRelocationEvent.taskRelocationPlanRemoved(taskId));
        }
    }
}
