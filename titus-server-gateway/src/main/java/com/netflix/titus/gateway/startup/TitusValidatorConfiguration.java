package com.netflix.titus.gateway.startup;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

public interface TitusValidatorConfiguration {
    @PropertyName(name = "titus.validate.job.timeoutMs")
    @DefaultValue("2000")
    String getTimeoutMs();
}
