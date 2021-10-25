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

package com.netflix.titus.runtime.connector.common.replicator;

import java.util.Objects;

public class ReplicatorEvent<SNAPSHOT, TRIGGER> {

    private final SNAPSHOT snapshot;
    private final TRIGGER trigger;
    private final long lastUpdateTime;
    private final long lastCheckpointTimestamp;

    public ReplicatorEvent(SNAPSHOT snapshot, TRIGGER trigger, long lastUpdateTime) {
        this(snapshot, trigger, lastUpdateTime, -1);
    }

    public ReplicatorEvent(SNAPSHOT snapshot, TRIGGER trigger, long lastUpdateTime, long lastCheckpointTimestamp) {
        this.snapshot = snapshot;
        this.trigger = trigger;
        this.lastUpdateTime = lastUpdateTime;
        this.lastCheckpointTimestamp = lastCheckpointTimestamp;
    }

    public SNAPSHOT getSnapshot() {
        return snapshot;
    }

    public TRIGGER getTrigger() {
        return trigger;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getLastCheckpointTimestamp() {
        return lastCheckpointTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicatorEvent<?, ?> that = (ReplicatorEvent<?, ?>) o;
        return lastUpdateTime == that.lastUpdateTime && lastCheckpointTimestamp == that.lastCheckpointTimestamp && Objects.equals(snapshot, that.snapshot) && Objects.equals(trigger, that.trigger);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, trigger, lastUpdateTime, lastCheckpointTimestamp);
    }

    @Override
    public String toString() {
        return "ReplicatorEvent{" +
                "snapshot=" + snapshot +
                ", trigger=" + trigger +
                ", lastUpdateTime=" + lastUpdateTime +
                ", lastCheckpointTimestamp=" + lastCheckpointTimestamp +
                '}';
    }
}
