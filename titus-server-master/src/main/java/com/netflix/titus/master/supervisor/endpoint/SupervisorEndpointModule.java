package com.netflix.titus.master.supervisor.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.TitusSupervisorServiceGrpc.TitusSupervisorServiceImplBase;
import com.netflix.titus.master.supervisor.endpoint.grpc.DefaultTitusSupervisorServiceGrpc;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;

public class SupervisorEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CallMetadataResolver.class).to(SimpleGrpcCallMetadataResolver.class);
        bind(TitusSupervisorServiceImplBase.class).to(DefaultTitusSupervisorServiceGrpc.class);
    }
}
