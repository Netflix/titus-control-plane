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

package io.netflix.titus.common.util.retry.internal;

import java.util.Optional;

import io.netflix.titus.common.util.retry.Retryer;

/**
 */
public class ImmediateRetryer implements Retryer {

    private static final Optional<Long> NO_DELAY = Optional.of(0L);

    public static final ImmediateRetryer UNLIMITED = new ImmediateRetryer(-1);

    private final int limit;

    public ImmediateRetryer(int limit) {
        this.limit = limit;
    }

    @Override
    public Optional<Long> getDelayMs() {
        return NO_DELAY;
    }

    @Override
    public Retryer retry() {
        if (limit == Integer.MAX_VALUE) {
            return this;
        }
        if (limit == 0) {
            return NeverRetryer.INSTANCE;
        }
        return new ImmediateRetryer(limit - 1);
    }
}
