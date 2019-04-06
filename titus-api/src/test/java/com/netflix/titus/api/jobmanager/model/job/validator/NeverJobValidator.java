/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job.validator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.EntityValidatorConfiguration;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

import java.util.Set;

/**
 * This validator never completes.  It is used to test validation in the face of unresponsive service calls.
 */
public class NeverJobValidator implements EntityValidator<JobDescriptor> {
    private final ValidationError.Type errorType;

    public NeverJobValidator() {
        this(ValidationError.Type.HARD);
    }

    public NeverJobValidator(ValidationError.Type errorType) {
        this.errorType = errorType;
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.never();
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) { return Mono.never(); }

    @Override
    public ValidationError.Type getErrorType(EntityValidatorConfiguration configuration) {
        return errorType;
    }
}
