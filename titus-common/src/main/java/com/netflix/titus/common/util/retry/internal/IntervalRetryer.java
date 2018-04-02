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
public class IntervalRetryer implements Retryer {

    private final Optional<Long> delayMs;
    private final int limit;

    public IntervalRetryer(Optional<Long> delayMs, int limit) {
        this.delayMs = delayMs;
        this.limit = limit;
    }

    @Override
    public Optional<Long> getDelayMs() {
        return delayMs;
    }

    @Override
    public Retryer retry() {
        if (limit == Integer.MAX_VALUE) {
            return this;
        }
        if (limit == 0) {
            return NeverRetryer.INSTANCE;
        }
        return new IntervalRetryer(delayMs, limit - 1);
    }
}
