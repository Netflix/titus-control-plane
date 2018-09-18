package com.netflix.titus.master.integration.v3.job;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used for testing purposes.
 */
public class FailJobEntityValidator implements EntityValidator<JobDescriptor> {
    public static final String ERR_MSG = "The FailJobEntityValidator should always fail with this error.";

    private static final ValidationError error = new ValidationError(ERR_MSG);
    private static final Set<ValidationError> errors = new HashSet<>(Arrays.asList(error));

    @Override
    public Set<ValidationError> validate(JobDescriptor entity) {
        return errors;
    }
}
