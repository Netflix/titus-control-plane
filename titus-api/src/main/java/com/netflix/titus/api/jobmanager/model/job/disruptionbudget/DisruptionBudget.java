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

package com.netflix.titus.api.jobmanager.model.job.disruptionbudget;

import java.util.List;
import java.util.Objects;

public class DisruptionBudget {

    private final DisruptionBudgetPolicy disruptionBudgetPolicy;
    private final DisruptionBudgetRate disruptionBudgetRate;
    private final List<TimeWindow> timeWindows;
    private final List<ContainerHealthProvider> containerHealthProviders;

    public DisruptionBudget(DisruptionBudgetPolicy disruptionBudgetPolicy,
                            DisruptionBudgetRate disruptionBudgetRate,
                            List<TimeWindow> timeWindows,
                            List<ContainerHealthProvider> containerHealthProviders) {
        this.disruptionBudgetPolicy = disruptionBudgetPolicy;
        this.disruptionBudgetRate = disruptionBudgetRate;
        this.timeWindows = timeWindows;
        this.containerHealthProviders = containerHealthProviders;
    }

    public DisruptionBudgetPolicy getDisruptionBudgetPolicy() {
        return disruptionBudgetPolicy;
    }

    public DisruptionBudgetRate getDisruptionBudgetRate() {
        return disruptionBudgetRate;
    }

    public List<TimeWindow> getTimeWindows() {
        return timeWindows;
    }

    public List<ContainerHealthProvider> getContainerHealthProviders() {
        return containerHealthProviders;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DisruptionBudget that = (DisruptionBudget) o;
        return Objects.equals(disruptionBudgetPolicy, that.disruptionBudgetPolicy) &&
                Objects.equals(disruptionBudgetRate, that.disruptionBudgetRate) &&
                Objects.equals(timeWindows, that.timeWindows) &&
                Objects.equals(containerHealthProviders, that.containerHealthProviders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(disruptionBudgetPolicy, disruptionBudgetRate, timeWindows, containerHealthProviders);
    }

    @Override
    public String toString() {
        return "DisruptionBudget{" +
                "disruptionBudgetPolicy=" + disruptionBudgetPolicy +
                ", disruptionBudgetRate=" + disruptionBudgetRate +
                ", timeWindows=" + timeWindows +
                ", containerHealthProviders=" + containerHealthProviders +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withContainerHealthProviders(containerHealthProviders)
                .withDisruptionBudgetPolicy(disruptionBudgetPolicy)
                .withDisruptionBudgetRate(disruptionBudgetRate)
                .withTimeWindows(timeWindows);
    }

    public static final class Builder {
        private DisruptionBudgetPolicy disruptionBudgetPolicy;
        private DisruptionBudgetRate disruptionBudgetRate;
        private List<TimeWindow> timeWindows;
        private List<ContainerHealthProvider> containerHealthProviders;

        private Builder() {
        }

        public Builder withDisruptionBudgetPolicy(DisruptionBudgetPolicy disruptionBudgetPolicy) {
            this.disruptionBudgetPolicy = disruptionBudgetPolicy;
            return this;
        }

        public Builder withDisruptionBudgetRate(DisruptionBudgetRate disruptionBudgetRate) {
            this.disruptionBudgetRate = disruptionBudgetRate;
            return this;
        }

        public Builder withTimeWindows(List<TimeWindow> timeWindows) {
            this.timeWindows = timeWindows;
            return this;
        }

        public Builder withContainerHealthProviders(List<ContainerHealthProvider> containerHealthProviders) {
            this.containerHealthProviders = containerHealthProviders;
            return this;
        }

        public Builder but() {
            return newBuilder().withDisruptionBudgetPolicy(disruptionBudgetPolicy).withDisruptionBudgetRate(disruptionBudgetRate).withTimeWindows(timeWindows).withContainerHealthProviders(containerHealthProviders);
        }

        public DisruptionBudget build() {
            return new DisruptionBudget(disruptionBudgetPolicy, disruptionBudgetRate, timeWindows, containerHealthProviders);
        }
    }
}