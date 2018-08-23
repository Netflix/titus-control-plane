package com.netflix.titus.ext.zookeeper.connector;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.runtime.health.guice.HealthModule;
import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;

public class ZookeeperConnectorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CuratorService.class).to(CuratorServiceImpl.class);
        bind(ZookeeperClusterResolver.class).to(DefaultZookeeperClusterResolver.class);

        install(new HealthModule() {
            @Override
            protected void configureHealth() {
                bindAdditionalHealthIndicator().to(ZookeeperHealthIndicator.class);
            }
        });
    }

    @Provides
    @Singleton
    public ZookeeperConfiguration getZookeeperConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ZookeeperConfiguration.class);
    }
}
