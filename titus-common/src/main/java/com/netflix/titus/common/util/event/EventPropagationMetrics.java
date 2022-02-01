/*
 * Copyright 2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,set
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.spectator.api.Id;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.ValueRangeCounter;

public class EventPropagationMetrics {

    private static final long[] LEVELS = new long[]{1, 10, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000};

    private final List<String> stageNames;

    private final Map<String, ValueRangeCounter> bucketCounters;
    private final Map<String, ValueRangeCounter> cumulativeCounters;

    public EventPropagationMetrics(Id root, List<String> stageNames, TitusRuntime titusRuntime) {
        this.stageNames = stageNames;
        this.bucketCounters = buildStageCounterMap(root, stageNames, titusRuntime, "inStage");
        this.cumulativeCounters = buildStageCounterMap(root, stageNames, titusRuntime, "toStage");
    }

    public void record(EventPropagationTrace trace) {
        long sum = 0;
        for (String name : stageNames) {
            Long delayMs = trace.getStages().get(name);
            long recordedDelayMs = delayMs == null ? 0L : delayMs;
            sum += recordedDelayMs;
            bucketCounters.get(name).recordLevel(recordedDelayMs);
            cumulativeCounters.get(name).recordLevel(sum);
        }
    }

    private Map<String, ValueRangeCounter> buildStageCounterMap(Id root, List<String> stageNames, TitusRuntime titusRuntime, String kind) {
        Map<String, ValueRangeCounter> bucketCounters = new HashMap<>();
        for (String name : stageNames) {
            bucketCounters.put(name, SpectatorExt.newValueRangeCounterSortable(
                    root.withTags("kind", kind, "stage", name),
                    LEVELS,
                    titusRuntime.getRegistry()
            ));
        }
        return bucketCounters;
    }
}
