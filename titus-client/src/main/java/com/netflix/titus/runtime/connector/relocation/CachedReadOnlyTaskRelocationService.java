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

package com.netflix.titus.runtime.connector.relocation;

import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.api.relocation.service.ReadOnlyTaskRelocationService;
import reactor.core.publisher.Flux;

@Singleton
public class CachedReadOnlyTaskRelocationService implements ReadOnlyTaskRelocationService {

    private final RelocationDataReplicator replicator;

    @Inject
    public CachedReadOnlyTaskRelocationService(RelocationDataReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public Map<String, TaskRelocationPlan> getPlannedRelocations() {
        return replicator.getCurrent().getPlans();
    }

    @Override
    public Flux<TaskRelocationEvent> events() {
        throw new IllegalStateException("method not implemented yet");
    }
}
