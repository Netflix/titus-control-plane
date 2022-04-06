package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import com.netflix.archaius.api.annotations.DefaultValue;

public interface GrpcCallAssistantConfiguration {

    @DefaultValue("10000")
    long getRequestTimeoutMs();

    @DefaultValue("1800000")
    long getStreamTimeoutMs();
}
