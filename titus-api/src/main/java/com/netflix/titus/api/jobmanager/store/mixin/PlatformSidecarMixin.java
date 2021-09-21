package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Struct;

public class PlatformSidecarMixin {
    @JsonCreator
    public PlatformSidecarMixin(
            @JsonProperty("name") String name,
            @JsonProperty("channel") String channel,
            @JsonProperty("arguments") Struct arguments
    ) {
    }
}
