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

package com.netflix.titus.api.jobmanager.model.job.retry;

public final class ExponentialBackoffRetryPolicyBuilder extends RetryPolicy.RetryPolicyBuilder<ExponentialBackoffRetryPolicy, ExponentialBackoffRetryPolicyBuilder> {

    private long initialDelayMs;
    private long maxDelayMs;

    ExponentialBackoffRetryPolicyBuilder() {
    }

    public ExponentialBackoffRetryPolicyBuilder withInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
        return this;
    }

    public ExponentialBackoffRetryPolicyBuilder withMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
        return this;
    }

    public ExponentialBackoffRetryPolicyBuilder but() {
        return ExponentialBackoffRetryPolicy.newBuilder().withInitialDelayMs(initialDelayMs).withMaxDelayMs(maxDelayMs).withRetries(retries);
    }

    public ExponentialBackoffRetryPolicy build() {
        return new ExponentialBackoffRetryPolicy(retries, initialDelayMs, maxDelayMs);
    }
}
