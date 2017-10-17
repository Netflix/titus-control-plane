package io.netflix.titus.api.appscale.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricDimensionMixin {
    @JsonCreator
    MetricDimensionMixin(
            @JsonProperty("name") String name,
            @JsonProperty("value") String value) {

    }
}
