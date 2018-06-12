package com.netflix.titus.gateway.eviction;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;

public class EvictionModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EvictionServiceGrpc.EvictionServiceImplBase.class).to(GatewayGrpcEvictionService.class);
        bind(EvictionServiceClient.class).to(GrpcEvictionServiceClient.class);
    }
}
