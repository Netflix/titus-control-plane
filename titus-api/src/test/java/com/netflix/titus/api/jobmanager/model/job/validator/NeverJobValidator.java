package com.netflix.titus.api.jobmanager.model.job.validator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

import java.util.Set;

/**
 * This validator never completes.  It is used to test validation in the face of unresponsive service calls.
 */
public class NeverJobValidator implements EntityValidator<JobDescriptor> {
    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.never();
    }
}
