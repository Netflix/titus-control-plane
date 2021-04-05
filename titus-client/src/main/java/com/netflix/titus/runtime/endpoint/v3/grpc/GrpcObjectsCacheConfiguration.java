package com.netflix.titus.runtime.endpoint.v3.grpc;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.grpc.cache")
public interface GrpcObjectsCacheConfiguration {

    /**
     * @return whether or not the cache is enabled
     */
    @DefaultValue("true")
    boolean isGrpcObjectsCacheEnabled();

    /**
     * @return the initial delay in milliseconds before the cache refresh controller runs
     */
    @DefaultValue("10000")
    long getCacheRefreshInitialDelayMs();

    /**
     * @return the interval in milliseconds of how often the cache refreshes
     */
    @DefaultValue("30000")
    long getCacheRefreshIntervalMs();

    /**
     * @return the timeout of the cache refresh controller loop.
     */
    @DefaultValue("60000")
    long getCacheRefreshTimeoutMs();
}
