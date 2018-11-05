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

package com.netflix.titus.runtime.connector.eviction;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.model.Tier;
import reactor.core.publisher.Flux;

@Singleton
public class CachedReadOnlyEvictionOperations implements ReadOnlyEvictionOperations {

    private final EvictionDataReplicator replicator;

    @Inject
    public CachedReadOnlyEvictionOperations(EvictionDataReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public EvictionQuota getSystemEvictionQuota() {
        return replicator.getCurrent().getSystemEvictionQuota();
    }

    @Override
    public EvictionQuota getTierEvictionQuota(Tier tier) {
        return replicator.getCurrent().getTierEvictionQuota(tier);
    }

    @Override
    public EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName) {
        return replicator.getCurrent()
                .findCapacityGroupEvictionQuota(capacityGroupName)
                .orElseThrow(() -> EvictionException.capacityGroupNotFound(capacityGroupName));
    }

    @Override
    public Optional<EvictionQuota> findJobEvictionQuota(String jobId) {
        throw new IllegalStateException("method not implemented yet");
    }

    @Override
    public Flux<EvictionEvent> events(boolean includeSnapshot) {
        throw new IllegalStateException("method not implemented yet");
    }
}
