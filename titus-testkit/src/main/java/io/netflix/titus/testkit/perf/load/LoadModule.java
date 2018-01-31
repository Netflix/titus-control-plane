package io.netflix.titus.testkit.perf.load;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;

public class LoadModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public LoadConfiguration getLoadConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(LoadConfiguration.class);
    }
}
