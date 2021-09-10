package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class SharedContainerVolumeSourceMixin {
    @JsonCreator
    public SharedContainerVolumeSourceMixin(
            @JsonProperty("sourceContainer") String sourceContainer,
            @JsonProperty("sourcePath") String sourcePath
    ) {
    }
}
