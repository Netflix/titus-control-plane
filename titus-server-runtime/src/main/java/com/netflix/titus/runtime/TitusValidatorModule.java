package com.netflix.titus.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.api.jobmanager.model.job.validator.PassJobValidator;
import com.netflix.titus.common.model.validator.Validator;

import javax.inject.Singleton;

/**
 * This module provides dependencies for Titus validation which is beyond syntactic validation.
 * See {@link TitusEntitySanitizerModule} for syntactic sanitization and validation.
 */
public class TitusValidatorModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public Validator getJobValidator() {
        return new PassJobValidator();
    }
}
