package io.netflix.titus.api.appscale.store.mixin;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PredefinedMetricSpecificationMixin {
    @JsonCreator
    PredefinedMetricSpecificationMixin(
            @JsonProperty("predefinedMetricType") String predefinedMetricType,
            @JsonProperty("resourceLabel") Optional<String> resourceLabel) {

    }
}
