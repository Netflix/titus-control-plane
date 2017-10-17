package io.netflix.titus.api.appscale.store.mixin;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.appscale.model.MetricDimension;
import io.netflix.titus.api.appscale.model.Statistic;

public class CustomizedMetricSpecificationMixin {
    @JsonCreator
    CustomizedMetricSpecificationMixin(
            @JsonProperty("metricDimensionList") List<MetricDimension> metricDimensionList,
            @JsonProperty("metricName") String metricName,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("statistic") Statistic statistic,
            @JsonProperty("unit") Optional<String> unit) {

    }
}
