package io.netflix.titus.api.appscale.store.mixin;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import io.netflix.titus.api.appscale.model.PredefinedMetricSpecification;

public class TargetTrackingPolicyMixin {
    @JsonCreator
    TargetTrackingPolicyMixin(
            @JsonProperty("targetValue") double targetValue,
            @JsonProperty("scaleOutCooldownSec") Optional<Integer> scaleOutCooldownSec,
            @JsonProperty("scaleInCooldownSec") Optional<Integer> scaleInCooldownSec,
            @JsonProperty("predefinedMetricSpecification") Optional<PredefinedMetricSpecification> predefinedMetricSpecification,
            @JsonProperty("disableScaleIn") Optional<Boolean> disableScaleIn,
            @JsonProperty("customizedMetricSpecification") Optional<CustomizedMetricSpecification> customizedMetricSpecification) {

    }
}
