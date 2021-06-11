package com.netflix.titus.federation.service;


import com.google.inject.Provides;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Named;
import javax.inject.Singleton;

@Configuration
public class JobActivityServiceComponent {

    public static final String JOB_ACTIVITY_HISTORY_CHANNEL = "JobActivityHistoryChannel";

    @Bean
    @Singleton
    @Named(JOB_ACTIVITY_HISTORY_CHANNEL)
    Channel jobActivityHistoryChannel(GrpcClientConfiguration configuration, TitusRuntime titusRuntime) {
            return NettyChannelBuilder.forTarget(configuration.getHostname()).build();
    }

    @Bean
    @Provides
    @Singleton
    JobActivityHistoryServiceStub jobActivityHistoryClient(final @Named(JOB_ACTIVITY_HISTORY_CHANNEL) Channel channel) {
        return JobActivityHistoryServiceGrpc.newStub(channel);
    }
}
