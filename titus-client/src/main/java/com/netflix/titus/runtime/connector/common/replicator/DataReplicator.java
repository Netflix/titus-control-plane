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

import java.io.Closeable;

import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;

/**
 * Data replicator from a remote service.
 */
public interface DataReplicator<SNAPSHOT extends ReplicatedSnapshot, TRIGGER> extends Closeable {
    /**
     * Get the latest known version of the data.
     */
    SNAPSHOT getCurrent();

    /**
     * Equivalent to calling "clock.wallTime() - getLastCheckpointTimestamp()".
     */
    long getStalenessMs();

    /**
     * Returns the timestamp for which all server side changes that happened before it are guaranteed to be in cache.
     * The actual cache state may be more up to date, so this is an upper bound on the replication latency.
     * Returns -1 if the value is not known.
     */
    long getLastCheckpointTimestamp();

    /**
     * Equivalent to "observeLastCheckpointTimestamp().map(timestamp -> clock.wallTime() - timestamp)".
     */
    Flux<Long> observeDataStalenessMs();

    /**
     * Emits a value when a new checkpoint marker is received.
     */
    Flux<Long> observeLastCheckpointTimestamp();

    /**
     * Emits events that triggered snapshot updates.
     */
    Flux<Pair<SNAPSHOT, TRIGGER>> events();
}
