/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This {@link AdmissionValidator} implementation always causes validation to fail.  It is used for testing purposes.
 */
public class FailJobValidator implements AdmissionValidator<JobDescriptor>, AdmissionSanitizer<JobDescriptor, JobDescriptor> {
    public static final String ERR_FIELD = "fail-field";
    public static final String ERR_DESCRIPTION = "The FailJobValidator should always fail with a unique error:";

    private final ValidationError.Type errorType;

    public FailJobValidator() {
        this(ValidationError.Type.HARD);
    }

    public FailJobValidator(ValidationError.Type errorType) {
        this.errorType = errorType;
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        final String errorMsg = String.format("%s %s", ERR_DESCRIPTION, UUID.randomUUID().toString());
        final ValidationError error = new ValidationError(ERR_FIELD, errorMsg);

        return Mono.just(new HashSet<>(Collections.singletonList(error)));
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        return Mono.error(TitusServiceException.invalidArgument(ERR_DESCRIPTION));
    }

    @Override
    public JobDescriptor apply(JobDescriptor entity, JobDescriptor update) {
        return entity;
    }

    @Override
    public ValidationError.Type getErrorType() {
        return errorType;
    }
}
