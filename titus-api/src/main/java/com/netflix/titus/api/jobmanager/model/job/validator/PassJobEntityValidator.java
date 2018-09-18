package com.netflix.titus.api.jobmanager.model.job.validator;

import java.util.Collections;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;

/**
 * This {@link EntityValidator} implementation always causes validation to fail.  It is used as a default implementation which
 * should be overriden.
 */
public class PassJobEntityValidator implements EntityValidator<JobDescriptor> {
    @Override
    public Set<ValidationError> validate(JobDescriptor entity) {
        return Collections.emptySet();
    }
}
