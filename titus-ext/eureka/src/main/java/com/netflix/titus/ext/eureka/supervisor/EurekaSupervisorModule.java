package com.netflix.titus.ext.eureka.supervisor;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;

public class EurekaSupervisorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalMasterInstanceResolver.class).to(EurekaLocalMasterInstanceResolver.class);
    }

    @Provides
    @Singleton
    public EurekaSupervisorConfiguration getEurekaSupervisorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(EurekaSupervisorConfiguration.class);
    }
}
