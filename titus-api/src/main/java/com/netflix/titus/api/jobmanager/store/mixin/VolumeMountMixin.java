package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VolumeMountMixin {
    @JsonCreator
    public VolumeMountMixin(
            @JsonProperty("volumeName") String volumeName,
            @JsonProperty("mountPath") String mountPath,
            @JsonProperty("mountPropagation") String mountPropagation,
            @JsonProperty("readOnly") Boolean readOnly,
            @JsonProperty("subPath") String subPath
    ) {
    }
}
