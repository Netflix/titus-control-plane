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

package io.netflix.titus.api.jobmanager.model.job.retry;

import javax.validation.constraints.Min;

/**
 */
public class ExponentialBackoffRetryPolicy extends RetryPolicy<ExponentialBackoffRetryPolicy, ExponentialBackoffRetryPolicyBuilder> {

    @Min(value = 0, message = "Delay cannot be negative")
    private final long initialDelayMs;

    @Min(value = 0, message = "Delay cannot be negative")
    private final long maxDelayMs;

    public ExponentialBackoffRetryPolicy(int retries, long initialDelayMs, long maxDelayMs) {
        super(retries);
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ExponentialBackoffRetryPolicy that = (ExponentialBackoffRetryPolicy) o;

        if (initialDelayMs != that.initialDelayMs) {
            return false;
        }
        return maxDelayMs == that.maxDelayMs;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (initialDelayMs ^ (initialDelayMs >>> 32));
        result = 31 * result + (int) (maxDelayMs ^ (maxDelayMs >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ExponentialBackoffRetryPolicy{" +
                "initialDelayMs=" + initialDelayMs +
                ", maxDelayMs=" + maxDelayMs +
                '}';
    }

    @Override
    public ExponentialBackoffRetryPolicyBuilder toBuilder() {
        return newBuilder()
                .withInitialDelayMs(initialDelayMs)
                .withMaxDelayMs(maxDelayMs)
                .withRetries(getRetries());
    }

    public static ExponentialBackoffRetryPolicyBuilder newBuilder() {
        return new ExponentialBackoffRetryPolicyBuilder();
    }

}
