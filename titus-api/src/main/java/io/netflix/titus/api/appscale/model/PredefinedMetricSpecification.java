package io.netflix.titus.api.appscale.model;

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
