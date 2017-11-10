package io.netflix.titus.runtime.endpoint.common.rest;

import com.netflix.archaius.api.annotations.DefaultValue;

public interface RestServerConfiguration {

    /**
     * Default is false
     *
     * @return whether or not to log verbose resource errors in the log file.
     */
    @DefaultValue("false")
    boolean isJaxrsErrorLoggingEnabled();
}
