package com.netflix.titus.master.eviction.endpoint.grpc;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceImplBase;

public class EvictionEndpointModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(EvictionServiceImplBase.class).to(GrpcEvictionService.class);
    }
}
