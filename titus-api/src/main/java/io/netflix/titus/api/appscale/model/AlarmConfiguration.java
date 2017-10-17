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

package io.netflix.titus.api.appscale.model;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AlarmConfiguration {

    private final String name;
    private final String region;
    private final Optional<Boolean> actionsEnabled;
    private final ComparisonOperator comparisonOperator;
    private final String autoScalingGroupName;
    private final int evaluationPeriods;
    private final int periodSec;
    private final double threshold;
    private final String metricNamespace;
    private final String metricName;
    private final Statistic statistic;

    public AlarmConfiguration(String name, String region, Optional<Boolean> actionsEnabled, ComparisonOperator comparisonOperator,
                              String autoScalingGroupName, int evaluationPeriods, int periodSec,
                              double threshold, String metricNamespace, String metricName, Statistic statistic) {
        this.name = name;
        this.region = region;
        this.actionsEnabled = actionsEnabled;
        // TODO(Andrew L): Change the parameter
        this.comparisonOperator = comparisonOperator;
        this.autoScalingGroupName = autoScalingGroupName;
        this.evaluationPeriods = evaluationPeriods;
        this.periodSec = periodSec;
        this.threshold = threshold;
        this.metricNamespace = metricNamespace;
        this.metricName = metricName;
        this.statistic = statistic;
    }

    @JsonProperty
    public String getRegion() {
        return region;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Optional<Boolean> getActionsEnabled() {
        return actionsEnabled;
    }

    @JsonProperty
    public ComparisonOperator getComparisonOperator() {
        return comparisonOperator;
    }

    @JsonProperty
    public String getAutoScalingGroupName() {
        return autoScalingGroupName;
    }

    @JsonProperty
    public int getEvaluationPeriods() {
        return evaluationPeriods;
    }

    @JsonProperty
    public int getPeriodSec() {
        return periodSec;
    }

    @JsonProperty
    public double getThreshold() {
        return threshold;
    }

    @JsonProperty
    public String getMetricNamespace() {
        return metricNamespace;
    }

    @JsonProperty
    public String getMetricName() {
        return metricName;
    }

    @JsonProperty
    public Statistic getStatistic() {
        return statistic;
    }

    @Override
    public String toString() {
        return "AlarmConfiguration{" +
                "name='" + name + '\'' +
                ", region='" + region + '\'' +
                ", actionsEnabled=" + actionsEnabled +
                ", comparisonOperator=" + comparisonOperator +
                ", autoScalingGroupName='" + autoScalingGroupName + '\'' +
                ", evaluationPeriods=" + evaluationPeriods +
                ", periodSec=" + periodSec +
                ", threshold=" + threshold +
                ", metricNamespace='" + metricNamespace + '\'' +
                ", metricName='" + metricName + '\'' +
                ", statistic=" + statistic +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static class Builder {
        private String name;
        private String region;
        private Optional<Boolean> actionsEnabled = Optional.empty();
        private ComparisonOperator comparisonOperator;
        private String autoScalingGroupName;
        private int evaluationPeriods;
        private int periodSec;
        private double threshold;
        private String metricNamespace;
        private String metricName;
        private Statistic statistic;

        private Builder() {

        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }


        public Builder withRegion(String region) {
            this.region = region;
            return this;
        }


        public Builder withActionsEnabled(Boolean actionsEnabled) {
            this.actionsEnabled = Optional.of(actionsEnabled);
            return this;
        }


        public Builder withComparisonOperator(ComparisonOperator comparisonOperator) {
            this.comparisonOperator = comparisonOperator;
            return this;
        }

        public Builder withAutoScalingGroupName(String autoScalingGroupName) {
            this.autoScalingGroupName = autoScalingGroupName;
            return this;
        }

        public Builder withEvaluationPeriods(int evaluationPeriods) {
            this.evaluationPeriods = evaluationPeriods;
            return this;
        }


        public Builder withPeriodSec(int periodSec) {
            this.periodSec = periodSec;
            return this;
        }


        public Builder withThreshold(double threshold) {
            this.threshold = threshold;
            return this;
        }

        public Builder withMetricNamespace(String metricNamespace) {
            this.metricNamespace = metricNamespace;
            return this;
        }

        public Builder withMetricName(String metricName) {
            this.metricName = metricName;
            return this;
        }

        public Builder withStatistic(Statistic staticstic) {
            this.statistic = staticstic;
            return this;
        }

        public AlarmConfiguration build() {
            return new AlarmConfiguration(name, region, actionsEnabled, comparisonOperator,
                    autoScalingGroupName, evaluationPeriods, periodSec,
                    threshold, metricNamespace, metricName, statistic);
        }

    }
}
