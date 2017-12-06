package io.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ServiceJobProcessesMixin {
    @JsonCreator
    public ServiceJobProcessesMixin(@JsonProperty("disableIncreaseDesired") boolean disableIncreaseDesired,
                                    @JsonProperty("disableDecreaseDesired") boolean disableDecreaseDesired) {
    }
}
