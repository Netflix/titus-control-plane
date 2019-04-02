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

package com.netflix.titus.runtime.connector.relocation.noop;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class NoOpRelocationServiceClient implements RelocationServiceClient {
    @Override
    public Mono<Optional<TaskRelocationPlan>> findTaskRelocationPlan(String taskId) {
        return Mono.error(new IllegalArgumentException("Not found"));
    }

    @Override
    public Mono<List<TaskRelocationPlan>> findTaskRelocationPlans(Set<String> taskIds) {
        return Mono.just(Collections.emptyList());
    }

    @Override
    public Flux<TaskRelocationEvent> events(TaskRelocationQuery query) {
        return Flux.never();
    }
}
