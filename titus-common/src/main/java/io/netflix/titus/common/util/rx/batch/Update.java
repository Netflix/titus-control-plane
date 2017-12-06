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

import java.time.Instant;
import java.util.Comparator;

/**
 * Represents an update that can be batched. Implementations of this class are highly encouraged to be immutable,
 * since their identifiers are held as keys in Maps.
 *
 * @param <I> type of the identifier of the update. Updates with the same identifier are grouped in a batch
 */
public interface Update<I> {
    final static Comparator<Update<?>> COMPARING_BY_PRIORITY = Comparator.comparing(Update::getPriority);

    static Comparator<Update<?>> byPriority() {
        return COMPARING_BY_PRIORITY;
    }

    /**
     * Two or more updates with equal identifiers (as defined by <tt>equals()</tt> and <tt>hashCode()</tt>) are
     * deduplicated in each batch. Multiple calls of this should return equivalent identifiers (as per <tt>equals()</tt>
     * and <tt>hashCode()</tt>).
     *
     * @return an identifier object that properly implements <tt>equals</tt> and <tt>hashCode()</tt>, so multiple
     * equivalent updates are deduplicated in each batch
     */
    I getIdentifier();

    /**
     * Updates with a lower priority (as defined by java.util.Comparable<Priority>) are dropped (ignored) when
     * another update with a higher priority has been already queued.
     */
    Priority getPriority();

    /**
     * Older updates are dropped (ignored) when there is already a more recent one with equals or higher priority.
     */
    Instant getTimestamp();

    /**
     * Indicates when two updates would leave the system in the same state.
     * <p>
     * Implementations are encouraged to ignore the timestamp, since this is used to avoid replacing existing updates
     * with a newer ones that would do the same. This is also used to avoid losing updates when they get replaced
     * concurrently as they are being emitted (flushed). See {@link RateLimitedBatcher} for more details.
     *
     * @param other update to compare to
     * @return true when both represent the same changes that would leave the system in the same state when applied
     */
    boolean isEquivalent(Update<?> other);
}
