package com.netflix.titus.federation.service;

import com.google.inject.Provides;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.grpc.protogen.TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Named;
import javax.inject.Singleton;

@Configuration
public class TitusAgentSecurityGroupServiceComponent {

    public static final String SECURITY_GROUP_SERVICE_CHANNEL = "SecurityGroupServiceChannel";

    @Bean
    @Singleton
    @Named(SECURITY_GROUP_SERVICE_CHANNEL)
    Channel securityGroupServiceChannel(GrpcClientConfiguration configuration, TitusRuntime titusRuntime) {
        return NettyChannelBuilder.forTarget(configuration.getHostname()).build();
    }

    @Bean
    @Provides
    @Singleton
    TitusAgentSecurityGroupServiceStub securityGroupClient(final @Named(SecurityGroupServiceChannel) Channel channel) {
        return TitusAgentSecurityGroupServiceGrpc.newStub(channel);
    }
}
