package com.netflix.titus.api.jobmanager.model.job.validator;

import java.util.Collections;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used as a default implementation which
 * should be overriden.
 */
public class PassJobValidator implements EntityValidator<JobDescriptor> {
    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.just(Collections.emptySet());
    }
}
