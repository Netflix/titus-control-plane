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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.Reference;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

class StubbedEvictionData {

    private static final EvictionQuota SYSTEM_EVICTION_QUOTA = EvictionQuota.newBuilder()
            .withQuota(Long.MAX_VALUE / 2)
            .withReference(Reference.system())
            .build();

    private EvictionQuota systemQuota = SYSTEM_EVICTION_QUOTA;
    private final ConcurrentMap<String, Long> jobQuota = new ConcurrentHashMap<>();

    private final EmitterProcessor<EvictionEvent> eventProcessor = EmitterProcessor.create();

    EvictionQuota getSystemEvictionQuota() {
        return systemQuota;
    }

    EvictionQuota getTierEvictionQuota(Tier tier) {
        return SYSTEM_EVICTION_QUOTA;
    }

    EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName) {
        return SYSTEM_EVICTION_QUOTA;
    }

    Optional<Long> findJobQuota(String jobId) {
        return Optional.ofNullable(jobQuota.get(jobId));
    }

    void setSystemQuota(int quota) {
        systemQuota = EvictionQuota.systemQuota(quota);
        eventProcessor.onNext(EvictionEvent.newQuotaEvent(systemQuota));
    }

    void setJobQuota(String jobId, long quota) {
        jobQuota.put(jobId, quota);
        eventProcessor.onNext(EvictionEvent.newQuotaEvent(EvictionQuota.jobQuota(jobId, quota)));
    }

    Flux<EvictionEvent> events(boolean includeSnapshot) {
        if (!includeSnapshot) {
            return eventProcessor;
        }
        return Flux.concat(
                Flux.just(EvictionEvent.newQuotaEvent(getSystemEvictionQuota())),
                newJobEvictionEventSnapshot(),
                Flux.just(EvictionEvent.newSnapshotEndEvent()),
                eventProcessor
        );
    }

    private Flux<EvictionEvent> newJobEvictionEventSnapshot() {
        return Flux
                .fromIterable(jobQuota.entrySet().stream()
                        .map(entry -> EvictionEvent.newQuotaEvent(EvictionQuota.jobQuota(entry.getKey(), entry.getValue())))
                        .collect(Collectors.toList())
                )
                .cast(EvictionEvent.class);
    }
}
