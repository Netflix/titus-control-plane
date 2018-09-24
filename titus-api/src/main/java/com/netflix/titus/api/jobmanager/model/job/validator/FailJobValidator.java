package com.netflix.titus.api.jobmanager.model.job.validator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used for testing purposes.
 */
public class FailJobValidator implements EntityValidator<JobDescriptor> {
    public static final String ERR_FIELD = "fail-field";
    public static final String ERR_DESCRIPTION = "The FailJobValidator should always fail with a unique error:";

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        final String errorMsg = String.format("%s %s", ERR_DESCRIPTION, UUID.randomUUID().toString());
        final ValidationError error = new ValidationError(ERR_FIELD, errorMsg);

        return Mono.just(new HashSet<>(Arrays.asList(error)));
    }
}
