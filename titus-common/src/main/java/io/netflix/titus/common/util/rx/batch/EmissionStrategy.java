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

package io.netflix.titus.common.util.rx.batch;

import java.util.Queue;
import java.util.stream.Stream;

/**
 * Defines the order in which batches should be emitted to downstream subscribers
 */
public interface EmissionStrategy {
    /**
     * Compute the order in which batches should be emitted.
     *
     * @param batches all batches to be emitted
     * @param <T>     type of updates in the batch
     * @param <I>     type of the index of each batch
     * @return a <tt>Queue</tt> representing the order in which batches should be emitted
     */
    <T extends Update<?>, I> Queue<Batch<T, I>> compute(Stream<Batch<T, I>> batches);
}

