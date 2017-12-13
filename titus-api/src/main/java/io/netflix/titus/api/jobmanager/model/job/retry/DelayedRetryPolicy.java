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

import java.util.concurrent.TimeUnit;
import javax.validation.constraints.Min;

/**
 */
public class DelayedRetryPolicy extends RetryPolicy<DelayedRetryPolicy, DelayedRetryPolicyBuilder> {

    @Min(value = 0, message = "Delay cannot be negative")
    private final long delayMs;

    public DelayedRetryPolicy(long delayMs, int retries) {
        super(retries);
        this.delayMs = delayMs;

    }

    public long getDelayMs() {
        return delayMs;
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

        DelayedRetryPolicy that = (DelayedRetryPolicy) o;

        return delayMs == that.delayMs;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (delayMs ^ (delayMs >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DelayedRetryPolicy{" +
                "retries=" + getRetries() +
                "delayMs=" + delayMs +
                '}';
    }

    @Override
    public DelayedRetryPolicyBuilder toBuilder() {
        return newBuilder()
                .withDelay(delayMs, TimeUnit.MILLISECONDS)
                .withRetries(getRetries());
    }

    public static DelayedRetryPolicyBuilder newBuilder() {
        return new DelayedRetryPolicyBuilder();
    }

}
