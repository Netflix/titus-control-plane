package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.netflix.titus.api.jobmanager.model.job.volume.VolumeSource;

public abstract class VolumeMixin {
    @JsonCreator
    public VolumeMixin(
            @JsonProperty("name") String name,
            @JsonProperty("volumeSource") VolumeSource volumeSource
    ) {
    }
}
