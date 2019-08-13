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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;

@Singleton
public class OpportunisticCpuCache {
    // agentId -> availability
    private volatile Map<String, OpportunisticCpuAvailability> snapshot = Collections.emptyMap();
    private final OpportunisticCpuAvailabilityProvider provider;
    private final TitusRuntime titusRuntime;

    @Inject
    public OpportunisticCpuCache(OpportunisticCpuAvailabilityProvider provider, TitusRuntime titusRuntime) {
        this.provider = provider;
        this.titusRuntime = titusRuntime;
    }

    public void prepare() {
        snapshot = provider.getOpportunisticCpus();
    }

    public Optional<String> findOpportunisticCpuAllocationId(String agentId) {
        return Optional.ofNullable(snapshot.get(agentId))
                .flatMap(availability -> availability.isExpired(titusRuntime.getClock()) ?
                        Optional.empty() : Optional.of(availability))
                .map(OpportunisticCpuAvailability::getAllocationId);
    }

}
