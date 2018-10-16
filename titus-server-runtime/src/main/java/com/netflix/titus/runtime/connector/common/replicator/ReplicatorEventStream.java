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

import reactor.core.publisher.Flux;

/**
 * An auxiliary interface used within the connector components only to deal with the remote Rx event streams.
 */
public interface ReplicatorEventStream<D> {

    long LATENCY_REPORT_INTERVAL_MS = 1_000;

    Flux<ReplicatorEvent<D>> connect();

    class ReplicatorEvent<D> {
        private final D data;
        private final long lastUpdateTime;

        public ReplicatorEvent(D data, long lastUpdateTime) {
            this.data = data;
            this.lastUpdateTime = lastUpdateTime;
        }

        public D getData() {
            return data;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicatorEvent<?> that = (ReplicatorEvent<?>) o;
            return lastUpdateTime == that.lastUpdateTime &&
                    Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data, lastUpdateTime);
        }

        @Override
        public String toString() {
            return "ReplicatorEvent{" +
                    "data=" + data +
                    ", lastUpdateTime=" + lastUpdateTime +
                    '}';
        }
    }
}
