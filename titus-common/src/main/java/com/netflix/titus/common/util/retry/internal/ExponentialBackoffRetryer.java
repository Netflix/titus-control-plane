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

package com.netflix.titus.common.util.retry.internal;

import java.util.Optional;

import com.netflix.titus.common.util.retry.Retryer;

/**
 */
public class ExponentialBackoffRetryer implements Retryer {

    private final Optional<Long> currentDelayMs;
    private final long maxDelayMs;
    private final int limit;

    public ExponentialBackoffRetryer(Optional<Long> currentDelayMs, long maxDelayMs, int limit) {
        this.currentDelayMs = currentDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.limit = limit;
    }

    @Override
    public Optional<Long> getDelayMs() {
        return currentDelayMs;
    }

    @Override
    public Retryer retry() {
        if (limit == Integer.MAX_VALUE && currentDelayMs.get() == maxDelayMs) {
            return this;
        }
        if (limit == 0) {
            return NeverRetryer.INSTANCE;
        }
        return new ExponentialBackoffRetryer(
                Optional.of(Math.min(currentDelayMs.get() * 2, maxDelayMs)),
                maxDelayMs,
                limit - 1
        );
    }
}
