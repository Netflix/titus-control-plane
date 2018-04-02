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

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PredefinedMetricSpecification {

    private final String predefinedMetricType;
    private final Optional<String> resourceLabel;


    public PredefinedMetricSpecification(String predefinedMetricType, Optional<String> resourceLabel) {
        this.predefinedMetricType = predefinedMetricType;
        this.resourceLabel = resourceLabel;
    }

    public String getPredefinedMetricType() {
        return predefinedMetricType;
    }

    public Optional<String> getResourceLabel() {
        return resourceLabel;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private String predefinedMetricType;
        private Optional<String> resourceLabel = Optional.empty();

        private Builder() {
        }

        public Builder withPredefinedMetricType(String predefinedMetricType) {
            this.predefinedMetricType = predefinedMetricType;
            return this;
        }

        public Builder withResourceLabel(String resourceLabel) {
            this.resourceLabel = Optional.of(resourceLabel);
            return this;
        }

        public PredefinedMetricSpecification build() {
            return new PredefinedMetricSpecification(predefinedMetricType, resourceLabel);
        }
    }
}
