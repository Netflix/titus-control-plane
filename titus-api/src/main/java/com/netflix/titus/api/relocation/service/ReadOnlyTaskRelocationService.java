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

package com.netflix.titus.api.relocation.service;

import java.util.Map;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import reactor.core.publisher.Flux;

public interface ReadOnlyTaskRelocationService {

    /**
     * Returns all currently planned task relocations. Currently this will include only tasks needing relocation
     * with self managed disruption policy.
     */
    Map<String, TaskRelocationPlan> getPlannedRelocations();

    /**
     * Task relocation events.
     */
    Flux<TaskRelocationEvent> events();
}
