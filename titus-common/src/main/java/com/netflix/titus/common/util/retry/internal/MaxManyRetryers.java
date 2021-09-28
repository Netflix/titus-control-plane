/*
 * Copyright 2021 Netflix, Inc.
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

public class MaxManyRetryers implements Retryer {

    private final Retryer[] retryers;

    public MaxManyRetryers(Retryer[] retryers) {
        this.retryers = retryers;
    }

    @Override
    public Optional<Long> getDelayMs() {
        long maxDelay = -1;
        for (Retryer retryer : retryers) {
            Optional<Long> next = retryer.getDelayMs();
            if (next.isPresent()) {
                long nextLong = next.get();
                if (nextLong > maxDelay) {
                    maxDelay = nextLong;
                }
            } else {
                return Optional.empty();
            }
        }
        return maxDelay < 0 ? Optional.empty() : Optional.of(maxDelay);
    }

    @Override
    public Retryer retry() {
        Retryer[] newRetryers = new Retryer[retryers.length];
        for (int i = 0; i < retryers.length; i++) {
            Retryer next = retryers[i].retry();
            if (next instanceof NeverRetryer) {
                return NeverRetryer.INSTANCE;
            }
            newRetryers[i] = next;
        }
        return new MaxManyRetryers(newRetryers);
    }
}
