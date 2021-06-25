package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
public abstract class NetworkConfigurationMixin {

    @JsonCreator
    public NetworkConfigurationMixin(
            @JsonProperty("networkMode") int networkMode) {
    }
}
