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

package com.netflix.titus.common.util.rx.batch;

import java.time.Instant;
import java.util.Comparator;

/**
 * Represents an item that can be batched, usually objects representing operations. Implementations of this class are
 * highly encouraged to be immutable, since their identifiers are held as keys in Maps.
 *
 * @param <I> type of the identifier. There will be at most one item for each identifier in a batch
 */
public interface Batchable<I> {
    final static Comparator<Batchable<?>> COMPARING_BY_PRIORITY = Comparator.comparing(Batchable::getPriority);

    static Comparator<Batchable<?>> byPriority() {
        return COMPARING_BY_PRIORITY;
    }

    /**
     * Two or more <tt>Batchables</tt> with equal identifiers (as defined by <tt>equals()</tt> and <tt>hashCode()</tt>)
     * are deduplicated in each batch. Multiple calls of this should return equivalent identifiers (as per <tt>equals()</tt>
     * and <tt>hashCode()</tt>).
     *
     * @return an identifier object that properly implements <tt>equals</tt> and <tt>hashCode()</tt>, so multiple
     * equivalent items are deduplicated in each batch
     */
    I getIdentifier();

    /**
     * <tt>Batchables</tt> with lower priorities (as defined by java.util.Comparable<Priority>) are dropped (ignored)
     * when another item with the same identifier and higher priority has been already queued.
     */
    Priority getPriority();

    /**
     * Older items are dropped (ignored) when there is already a more recent one (same identifier) with the same or
     * higher priority.
     */
    Instant getTimestamp();

    /**
     * Indicates when two operations would leave the system in the same state.
     * <p>
     * Implementations are encouraged to ignore the timestamp, since this is used to avoid replacing existing items
     * with a newer ones that would do the same. This is also used to avoid losing items when they get replaced
     * concurrently as they are being emitted (flushed). See {@link RateLimitedBatcher} for more details.
     *
     * @param other <tt>Batchable</tt> to compare to
     * @return true when both represent the same changes that would leave the system in the same state when applied
     */
    boolean isEquivalent(Batchable<?> other);
}
