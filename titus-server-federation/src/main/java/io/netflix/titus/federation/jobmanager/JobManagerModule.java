package io.netflix.titus.federation.jobmanager;

import com.google.inject.AbstractModule;
import io.netflix.titus.federation.jobmanager.endpoint.JobManagerEndpointModule;
import io.netflix.titus.federation.jobmanager.service.JobManagerServiceModule;

public class JobManagerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new JobManagerServiceModule());
        install(new JobManagerEndpointModule());
    }
}
