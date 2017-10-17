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

package io.netflix.titus.common.util.limiter;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import io.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import io.netflix.titus.common.util.limiter.tokenbucket.internal.DefaultTokenBucket;
import io.netflix.titus.common.util.limiter.tokenbucket.internal.FixedIntervalRefillStrategy;

public class Limiters {

    private Limiters() {
    }

    /**
     * Create a {@link TokenBucket} with a fixed interval {@link RefillStrategy}.
     */
    public static TokenBucket createFixedIntervalTokenBucket(String name, long capacity, long initialNumberOfTokens,
                                                             long numberOfTokensPerInterval, long interval, TimeUnit unit) {
        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(Stopwatch.createStarted(),
                numberOfTokensPerInterval, interval, unit);
        TokenBucket tokenBucket = new DefaultTokenBucket(name, capacity, refillStrategy, initialNumberOfTokens);

        return tokenBucket;
    }

}
