package io.netflix.titus.api.appscale.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricDimension {

    private String name;
    private String value;

    public MetricDimension(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getValue() {
        return value;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private String name;
        private String value;

        private Builder() {
        }

        public static Builder aMetricDimension() {
            return new Builder();
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public MetricDimension build() {
            return new MetricDimension(name, value);
        }
    }
}
