package io.netflix.titus.federation.jobmanager.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;

public final class JobManagerEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JobManagementServiceGrpc.JobManagementServiceImplBase.class).toInstance(
                new JobManagementServiceGrpc.JobManagementServiceImplBase() {
                }
        );
    }
}
