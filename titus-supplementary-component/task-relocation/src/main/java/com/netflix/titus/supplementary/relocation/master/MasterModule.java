package com.netflix.titus.supplementary.relocation.master;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.connector.jobmanager.JobCacheResolver;
import com.netflix.titus.runtime.connector.jobmanager.cache.DefaultJobCacheResolver;

public class MasterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JobCacheResolver.class).to(DefaultJobCacheResolver.class);
        bind(TitusMasterOperations.class).to(DefaultTitusMasterOperations.class).asEagerSingleton();
    }
}
