package io.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExponentialBackoffRetryPolicyMixin {

    @JsonCreator
    public ExponentialBackoffRetryPolicyMixin(@JsonProperty("retries") int retries,
                                              @JsonProperty("initialDelayMs") long initialDelayMs,
                                              @JsonProperty("maxDelayMs") long maxDelayMs) {
    }
}
