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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.event;

import java.util.Map;
import java.util.Objects;

public class EventPropagationTrace {

    private final boolean snapshot;
    private final Map<String, Long> stages;
    private final long totalDelayMs;

    public EventPropagationTrace(boolean snapshot,
                                 Map<String, Long> stages) {
        this.snapshot = snapshot;
        this.stages = stages;
        long total = 0;
        for(Long value : stages.values()) {
            total += value;
        }
        this.totalDelayMs = total;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    public Map<String, Long> getStages() {
        return stages;
    }

    public long getTotalDelayMs() {
        return totalDelayMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventPropagationTrace that = (EventPropagationTrace) o;
        return snapshot == that.snapshot && totalDelayMs == that.totalDelayMs && Objects.equals(stages, that.stages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, stages, totalDelayMs);
    }

    @Override
    public String toString() {
        return "EventPropagationTrace{" +
                "snapshot=" + snapshot +
                ", stages=" + stages +
                ", totalDelayMs=" + totalDelayMs +
                '}';
    }
}
