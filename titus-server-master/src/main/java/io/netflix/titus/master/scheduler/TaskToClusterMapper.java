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

package io.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.QueuableTask;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.model.Tier;

class TaskToClusterMapper {
    private volatile Func1<QueuableTask, List<String>> mapperFunc1 = task -> Collections.emptyList();

    // Set up the mapper so it is able to provide answers quickly when called from Fenzo.
    void update(AgentManagementService agentManagementService) {
        final Map<Integer, Map<Boolean, List<String>>> tierSplit = new HashMap<>();
        Stream.of(Tier.values()).forEach(tier -> {
            HashMap<Boolean, List<String>> gpuNonGpu = new HashMap<>();
            gpuNonGpu.put(false, new ArrayList<>());
            gpuNonGpu.put(true, new ArrayList<>());
            tierSplit.put(tier.ordinal(), gpuNonGpu);
        });

        agentManagementService.getInstanceGroups().forEach(instanceGroup -> {
            boolean hasGpu = instanceGroup.getResourceDimension().getGpu() > 0;
            tierSplit.get(instanceGroup.getTier().ordinal()).get(hasGpu).add(instanceGroup.getId());
        });

        mapperFunc1 = task -> {
            final Map<Boolean, List<String>> tierMap = tierSplit.get(task.getQAttributes().getTierNumber());
            if (tierMap == null || tierMap.isEmpty()) {
                return Collections.emptyList();
            }
            return SchedulerUtils.hasGpuRequest(task) ? tierMap.get(true) : tierMap.get(false);
        };
    }

    Func1<QueuableTask, List<String>> getMapperFunc1() {
        return mapperFunc1;
    }
}
