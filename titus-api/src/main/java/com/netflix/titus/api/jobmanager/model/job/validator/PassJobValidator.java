package com.netflix.titus.api.jobmanager.model.job.validator;

import java.util.Collections;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Flux;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used as a default implementation which
 * should be overriden.
 */
public class PassJobValidator implements EntityValidator<JobDescriptor> {
    @Override
    public Flux<Set<ValidationError>> validate(JobDescriptor entity) {
        return Flux.just(Collections.emptySet());
    }
}
