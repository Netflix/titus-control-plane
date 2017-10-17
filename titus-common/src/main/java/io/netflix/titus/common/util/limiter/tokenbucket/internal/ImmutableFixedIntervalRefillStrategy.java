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

package io.netflix.titus.common.util.limiter.tokenbucket.internal;

import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket.ImmutableRefillStrategy;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.tuple.Pair;

/**
 */
public class ImmutableFixedIntervalRefillStrategy implements ImmutableRefillStrategy {

    private final long numberOfTokensPerInterval;
    private final long intervalNs;

    private final Clock clock;
    private final long startTimeNs;

    private final Pair<ImmutableRefillStrategy, Long> noProgressReply;

    public ImmutableFixedIntervalRefillStrategy(long numberOfTokensPerInterval, long intervalNs, Clock clock) {
        this.numberOfTokensPerInterval = numberOfTokensPerInterval;
        this.intervalNs = intervalNs;
        this.clock = clock;
        this.startTimeNs = clock.nanoTime();
        this.noProgressReply = Pair.of(this, 0L);
    }

    @Override
    public Pair<ImmutableRefillStrategy, Long> consume() {
        long refills = getRefills();
        if (refills == 0) {
            return noProgressReply;
        }
        return Pair.of(new ImmutableFixedIntervalRefillStrategy(numberOfTokensPerInterval, intervalNs, clock), refills);
    }

    private long getRefills() {
        long elapsed = clock.nanoTime() - startTimeNs;
        return elapsed / intervalNs;
    }
}
