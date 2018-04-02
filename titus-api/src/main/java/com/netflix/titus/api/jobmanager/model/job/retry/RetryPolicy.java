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

import javax.validation.constraints.Min;

/**
 */
public abstract class RetryPolicy<P extends RetryPolicy<P, B>, B extends RetryPolicy.RetryPolicyBuilder<P, B>> {

    @Min(value = 0, message = "Required 0 or a positive number")
    private final int retries;

    protected RetryPolicy(int retries) {
        this.retries = retries;
    }

    public int getRetries() {
        return retries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RetryPolicy that = (RetryPolicy) o;

        return retries == that.retries;
    }

    @Override
    public int hashCode() {
        return retries;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "retries=" + retries +
                '}';
    }

    public abstract B toBuilder();

    public static abstract class RetryPolicyBuilder<P extends RetryPolicy<P, B>, B extends RetryPolicyBuilder<P, B>> {

        protected int retries;

        public B withRetries(int retries) {
            this.retries = retries;
            return self();
        }

        public abstract P build();

        private B self() {
            return (B) this;
        }
    }
}
