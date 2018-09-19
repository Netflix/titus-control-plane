package com.netflix.titus.master.integration.v3.job;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used for testing purposes.
 */
public class FailJobValidator implements EntityValidator<JobDescriptor> {
    public static final String ERR_FIELD = "fail-field";
    public static final String ERR_DESCRIPTION = "The FailJobValidator should always fail with this error.";

    private static final ValidationError error = new ValidationError(ERR_FIELD, ERR_DESCRIPTION);
    private static final Set<ValidationError> errors = new HashSet<>(Arrays.asList(error));

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.just(errors);
    }
}
