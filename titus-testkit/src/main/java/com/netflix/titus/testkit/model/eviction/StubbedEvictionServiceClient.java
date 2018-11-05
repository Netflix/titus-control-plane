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

package com.netflix.titus.testkit.model.eviction;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class StubbedEvictionServiceClient implements EvictionServiceClient {

    private final EvictionOperations evictionOperations;

    StubbedEvictionServiceClient(EvictionOperations evictionOperations) {
        this.evictionOperations = evictionOperations;
    }

    @Override
    public Mono<EvictionQuota> getEvictionQuota(Reference reference) {
        EvictionQuota quota;
        switch (reference.getLevel()) {
            case System:
                quota = evictionOperations.getSystemEvictionQuota();
                break;
            case Tier:
                quota = evictionOperations.getTierEvictionQuota(Tier.valueOf(reference.getName()));
                break;
            case CapacityGroup:
                quota = evictionOperations.getCapacityGroupEvictionQuota(reference.getName());
                break;
            case Job:
                quota = evictionOperations.findJobEvictionQuota(reference.getName()).orElseGet(() -> EvictionQuota.emptyQuota(reference));
                break;
            case Task:
            default:
                quota = EvictionQuota.emptyQuota(reference);
        }
        return Mono.just(quota);
    }

    @Override
    public Mono<Void> terminateTask(String taskId, String reason) {
        return evictionOperations.terminateTask(taskId, reason);
    }

    @Override
    public Flux<EvictionEvent> observeEvents(boolean includeSnapshot) {
        return evictionOperations.events(includeSnapshot);
    }
}
