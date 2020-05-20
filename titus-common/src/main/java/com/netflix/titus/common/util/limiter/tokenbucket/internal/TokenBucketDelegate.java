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

import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

public class TokenBucketDelegate implements TokenBucket {

    private final TokenBucket delegate;

    public TokenBucketDelegate(TokenBucket delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public long getCapacity() {
        return delegate.getCapacity();
    }

    @Override
    public long getNumberOfTokens() {
        return delegate.getNumberOfTokens();
    }

    @Override
    public boolean tryTake() {
        return delegate.tryTake();
    }

    @Override
    public boolean tryTake(long numberOfTokens) {
        return delegate.tryTake(numberOfTokens);
    }

    @Override
    public void take() {
        delegate.take();
    }

    @Override
    public void take(long numberOfTokens) {
        delegate.take(numberOfTokens);
    }

    @Override
    public void refill(long numberOfToken) {
        delegate.refill(numberOfToken);
    }

    @Override
    public RefillStrategy getRefillStrategy() {
        return delegate.getRefillStrategy();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
