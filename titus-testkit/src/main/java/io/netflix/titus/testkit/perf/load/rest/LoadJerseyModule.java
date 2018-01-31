package io.netflix.titus.testkit.perf.load.rest;

import java.util.function.UnaryOperator;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.governator.guice.jersey.GovernatorServletContainer;
import com.netflix.governator.providers.Advises;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;

public class LoadJerseyModule extends JerseyServletModule {
    @Override
    protected void configureServlets() {
        // This sets up Jersey to serve any found resources that start with the base path of "/*"
        serve("/*").with(GovernatorServletContainer.class);
    }

    @Advises
    @Singleton
    @Named("governator")
    UnaryOperator<DefaultResourceConfig> getConfig() {
        return config -> {
            // Providers
            config.getClasses().add(JsonMessageReaderWriter.class);

            // Resources
            config.getClasses().add(ReportResource.class);
            return config;
        };
    }
}
