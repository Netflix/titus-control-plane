/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.embedded.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;

import io.netflix.titus.master.service.management.CapacityAllocationService;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Singleton
public class EmbeddedCapacityAllocationService implements CapacityAllocationService {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedCapacityAllocationService.class);

    private final Map<String, Integer> instanceTypeCounts = new HashMap<>();

    void addAgentCluster(SimulatedTitusAgentCluster simulatedAgentCluster) {
        String instanceType = simulatedAgentCluster.getInstanceType().getDescriptor().getId();
        int current = instanceTypeCounts.getOrDefault(instanceType, 0);
        instanceTypeCounts.put(instanceType, current + simulatedAgentCluster.getAgents().size());
    }

    @Override
    public Observable<List<Integer>> limits(List<String> instanceTypes) {
        List<Integer> result = new ArrayList<>(instanceTypes.size());
        instanceTypes.forEach(t -> result.add(instanceTypeCounts.getOrDefault(t, 0)));
        return Observable.just(result);
    }

    @Override
    public Observable<Void> allocate(String instanceType, int instanceCount) {
        logger.info("Capacity change request {}={}", instanceType, instanceCount);
        return Observable.empty();
    }
}
