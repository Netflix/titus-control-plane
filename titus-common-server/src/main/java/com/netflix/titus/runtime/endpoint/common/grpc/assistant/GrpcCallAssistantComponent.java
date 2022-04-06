package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcCallAssistantComponent {

    @Bean
    public GrpcClientCallAssistantFactory getGrpcClientCallAssistantFactory(GrpcCallAssistantConfiguration configuration,
                                                                            CallMetadataResolver callMetadataResolver) {
        return new DefaultGrpcClientCallAssistantFactory(configuration, callMetadataResolver);
    }

    @Bean
    public GrpcServerCallAssistant getGrpcServerCallAssistant(CallMetadataResolver callMetadataResolver,
                                                              HostCallerIdResolver hostCallerIdResolver) {
        return new DefaultGrpcServerCallAssistant(callMetadataResolver, hostCallerIdResolver);
    }
}
