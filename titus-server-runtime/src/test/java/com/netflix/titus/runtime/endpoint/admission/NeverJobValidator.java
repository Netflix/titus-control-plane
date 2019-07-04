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

import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This validator never completes.  It is used to test validation in the face of unresponsive service calls.
 */
class NeverJobValidator implements AdmissionValidator<JobDescriptor> {
    private final ValidationError.Type errorType;

    public NeverJobValidator() {
        this(ValidationError.Type.HARD);
    }

    NeverJobValidator(ValidationError.Type errorType) {
        this.errorType = errorType;
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.never();
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        return Mono.never();
    }

    @Override
    public ValidationError.Type getErrorType() {
        return errorType;
    }
}
