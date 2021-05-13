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

package com.netflix.titus.common.framework.simplereconciler;

import java.time.Duration;
import java.util.Objects;

/**
 * {@link ReconcilerActionProviderPolicy} provides evaluation rules and ordering. Reconciler actions come from two
 * main sources: external (user actions), and internal. Only actions coming from one source can be processed at a time.
 */
public class ReconcilerActionProviderPolicy {

    /**
     * Priority assigned to the external change actions. Internal action providers with a lower priority value will be always
     * executed before this one.
     */
    public static final int DEFAULT_EXTERNAL_ACTIONS_PRIORITY = 100;

    /**
     * Default policy assigned to the external change action provider.
     */
    private static final ReconcilerActionProviderPolicy DEFAULT_EXTERNAL_ACTION_PROVIDER_POLICY = newBuilder()
            .withPriority(DEFAULT_EXTERNAL_ACTIONS_PRIORITY)
            .withExecutionInterval(Duration.ZERO)
            .build();

    /**
     * Execution priority. The lower the value, the higher the priority.
     */
    private final int priority;

    /**
     * Evaluation frequency. It is guaranteed the time between a subsequent provider evaluations is at least the
     * configured interval value. But the actual time might be longer if a processing takes a lot of time or there
     * are other action providers with a higher priority. The time interval is measured between the last action
     * completion time and the next action start time.
     */
    private final Duration executionInterval;

    /**
     * Minimum execution frequency. If this limit is crossed, the provider priority is boosted to the top level.
     * If there are many action providers at the top level, they are executed in the round robin fashion.
     * If set to zero, the constraint is ignored.
     */
    private final Duration minimumExecutionInterval;

    public ReconcilerActionProviderPolicy(int priority,
                                          Duration executionInterval,
                                          Duration minimumExecutionInterval) {
        this.priority = priority;
        this.executionInterval = executionInterval;
        this.minimumExecutionInterval = minimumExecutionInterval;
    }

    public int getPriority() {
        return priority;
    }

    public Duration getExecutionInterval() {
        return executionInterval;
    }

    public static ReconcilerActionProviderPolicy getExternalChangeActionProvider() {
        return DEFAULT_EXTERNAL_ACTION_PROVIDER_POLICY;
    }

    public Duration getMinimumExecutionInterval() {
        return minimumExecutionInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReconcilerActionProviderPolicy that = (ReconcilerActionProviderPolicy) o;
        return priority == that.priority && Objects.equals(executionInterval, that.executionInterval) && Objects.equals(minimumExecutionInterval, that.minimumExecutionInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priority, executionInterval, minimumExecutionInterval);
    }

    @Override
    public String toString() {
        return "ReconcilerActionProviderPolicy{" +
                "priority=" + priority +
                ", executionInterval=" + executionInterval +
                ", minimumExecutionInterval=" + minimumExecutionInterval +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return newBuilder().withPriority(priority)
                .withExecutionInterval(executionInterval)
                .withMinimumExecutionInterval(minimumExecutionInterval);
    }

    public static final class Builder {

        private int priority;
        private Duration executionInterval;
        private Duration minimumExecutionInterval;

        private Builder() {
        }

        public Builder withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder withExecutionInterval(Duration executionInterval) {
            this.executionInterval = executionInterval;
            return this;
        }

        public Builder withMinimumExecutionInterval(Duration minimumExecutionInterval) {
            this.minimumExecutionInterval = minimumExecutionInterval;
            return this;
        }

        public ReconcilerActionProviderPolicy build() {
            return new ReconcilerActionProviderPolicy(priority, executionInterval, minimumExecutionInterval);
        }
    }
}
