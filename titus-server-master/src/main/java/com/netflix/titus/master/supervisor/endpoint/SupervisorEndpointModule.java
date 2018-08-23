package com.netflix.titus.master.supervisor.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceImplBase;
import com.netflix.titus.master.supervisor.endpoint.grpc.DefaultSupervisorServiceGrpc;

public class SupervisorEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SupervisorServiceImplBase.class).to(DefaultSupervisorServiceGrpc.class);
    }
}
