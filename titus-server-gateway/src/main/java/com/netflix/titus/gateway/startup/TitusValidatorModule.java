package com.netflix.titus.gateway.startup;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.jobmanager.model.job.validator.ParallelValidator;
import com.netflix.titus.api.jobmanager.model.job.validator.PassJobValidator;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;

import javax.inject.Singleton;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

/**
 * This module provides dependencies for Titus validation ({@link EntityValidator}) which is beyond syntactic
 * validation.  See {@link TitusEntitySanitizerModule} for syntactic sanitization and validation.
 */
public class TitusValidatorModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    TitusValidatorConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusValidatorConfiguration.class);
    }

    @Provides
    @Singleton
    public EntityValidator getJobValidator(TitusValidatorConfiguration configuration) {
        return new ParallelValidator(
                Duration.ofMillis(Long.parseLong(configuration.getTimeoutMs())),
                Collections.emptyList(), // No HARD validators
                Arrays.asList(new PassJobValidator()));
    }
}
