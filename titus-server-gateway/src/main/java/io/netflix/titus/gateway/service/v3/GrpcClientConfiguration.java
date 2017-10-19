package io.netflix.titus.gateway.service.v3;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.grpcClient")
public interface GrpcClientConfiguration {

    /**
     * GRPC operation timeout.
     */
    @DefaultValue("10000")
    long getRequestTimeout();
}
