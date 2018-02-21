package io.netflix.titus.federation.endpoint;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.federation.endpoint")
public interface EndpointConfiguration {

    @DefaultValue("7104")
    int getGrpcPort();

    /**
     * Graceful shutdown time for GRPC server. If zero, shutdown happens immediately, and all client connections are
     * terminated abruptly.
     */
    @DefaultValue("30000")
    long getGrpcServerShutdownTimeoutMs();
}
