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
     * Returns the number of milliseconds since the last data refresh time.
     */
    long getStalenessMs();

    /**
     * Emits periodically the number of milliseconds since the last data refresh time. Emits an error when the
     * cache refresh process fails, and cannot resume.
     */
    Flux<Long> observeDataStalenessMs();

    /**
     * Emits events that triggered snapshot updates.
     */
    Flux<Pair<SNAPSHOT, TRIGGER>> events();
}
