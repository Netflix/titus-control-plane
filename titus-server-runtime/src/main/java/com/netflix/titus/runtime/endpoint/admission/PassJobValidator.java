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
import java.util.Set;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This {@link AdmissionValidator} implementation always causes validation to pass.  It is used as a default implementation which
 * should be overridden.
 */
@Singleton
public class PassJobValidator implements AdmissionValidator<JobDescriptor>, AdmissionSanitizer<JobDescriptor> {
    private final ValidationError.Type errorType;

    public PassJobValidator() {
        this(ValidationError.Type.SOFT);
    }

    public PassJobValidator(ValidationError.Type errorType) {
        this.errorType = errorType;
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        return Mono.just(Collections.emptySet());
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        return Mono.just(entity);
    }

    @Override
    public ValidationError.Type getErrorType() {
        return errorType;
    }
}
