/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.limiter.tokenbucket.internal;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

public class SpectatorTokenBucketDecorator extends TokenBucketDelegate {

    private static final String NAME_PREFIX = "titus.common.tokenBucket.";

    private final Counter takeSuccessCounter;
    private final Counter takeFailureCounter;

    public SpectatorTokenBucketDecorator(TokenBucket delegate, TitusRuntime titusRuntime) {
        super(delegate);

        Registry registry = titusRuntime.getRegistry();

        this.takeSuccessCounter = registry.counter(
                NAME_PREFIX + "tokenRequest",
                "bucketName", delegate.getName(),
                "success", "true"
        );
        this.takeFailureCounter = registry.counter(
                NAME_PREFIX + "tokenRequest",
                "bucketName", delegate.getName(),
                "success", "false"
        );

        PolledMeter.using(registry).withId(registry.createId(
                NAME_PREFIX + "capacity",
                "bucketName", delegate.getName(),
                "available", "false"
        )).monitorValue(this, self -> self.getCapacity() - self.getNumberOfTokens());
        PolledMeter.using(registry).withId(registry.createId(
                NAME_PREFIX + "capacity",
                "bucketName", delegate.getName(),
                "available", "true"
        )).monitorValue(this, TokenBucketDelegate::getNumberOfTokens);
    }

    @Override
    public boolean tryTake() {
        boolean success = super.tryTake();
        recordTokenRequest(success, 1);
        return success;
    }

    @Override
    public boolean tryTake(long numberOfTokens) {
        boolean success = super.tryTake(numberOfTokens);
        recordTokenRequest(success, numberOfTokens);
        return success;
    }

    @Override
    public void take() {
        super.take();
        recordTokenRequest(true, 1);
    }

    @Override
    public void take(long numberOfTokens) {
        super.take(numberOfTokens);
        recordTokenRequest(true, numberOfTokens);
    }

    private void recordTokenRequest(boolean success, long count) {
        if (success) {
            takeSuccessCounter.increment(count);
        } else {
            takeFailureCounter.increment(count);
        }
    }
}
