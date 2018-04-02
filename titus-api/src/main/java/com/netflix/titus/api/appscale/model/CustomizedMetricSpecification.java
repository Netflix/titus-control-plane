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

package com.netflix.titus.api.appscale.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CustomizedMetricSpecification {
    private final List<MetricDimension> metricDimensionList;
    private final String metricName;
    private final String namespace;
    private final Statistic statistic;
    private final Optional<String> unit;

    public CustomizedMetricSpecification(List<MetricDimension> metricDimensionList, String metricName, String namespace, Statistic statistic, Optional<String> unit) {
        this.metricDimensionList = metricDimensionList;
        this.metricName = metricName;
        this.namespace = namespace;
        this.statistic = statistic;
        this.unit = unit;
    }

    public List<MetricDimension> getMetricDimensionList() {
        return metricDimensionList;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getNamespace() {
        return namespace;
    }

    public Statistic getStatistic() {
        return statistic;
    }

    public Optional<String> getUnit() {
        return unit;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private List<MetricDimension> metricDimensionList = new ArrayList<>();
        private String metricName;
        private String namespace;
        private Statistic statistic;
        private Optional<String> unit = Optional.empty();

        private Builder() {
        }

        public static Builder aCustomizedMetricSpecification() {
            return new Builder();
        }

        public Builder withMetricDimensionList(List<MetricDimension> metricDimensionList) {
            this.metricDimensionList = metricDimensionList;
            return this;
        }

        public Builder withMetricName(String metricName) {
            this.metricName = metricName;
            return this;
        }

        public Builder withNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder withStatistic(Statistic statistic) {
            this.statistic = statistic;
            return this;
        }

        public Builder withUnit(String unit) {
            this.unit = Optional.of(unit);
            return this;
        }

        public CustomizedMetricSpecification build() {
            return new CustomizedMetricSpecification(metricDimensionList, metricName, namespace, statistic, unit);
        }
    }
}
