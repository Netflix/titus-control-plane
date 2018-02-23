package io.netflix.titus.federation.endpoint.grpc;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.common.grpc.SessionContext;

public class GrpcModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TitusFederationGrpcServer.class).asEagerSingleton();
        bind(SessionContext.class).to(AnonymousSessionContext.class);
        bind(JobManagementServiceGrpc.JobManagementServiceImplBase.class).to(DefaultJobManagementGrpc.class);
    }
}
