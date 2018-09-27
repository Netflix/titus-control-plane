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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This {@link EntityValidator} implementation ensures a job's appname matches a specific
 * string. It is only used for testing purposes.
 */
public class AppNameSanitizer implements EntityValidator<JobDescriptor> {
    public final static String desiredAppName = "desiredAppName";
    private static final String ERR_FIELD = "fail-field";
    private static final String ERR_DESCRIPTION = "The job does not have desired appname " + desiredAppName;

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
        if (entity.getApplicationName().equals(desiredAppName)) {
            return Mono.just(Collections.emptySet());
        }
        final ValidationError error = new ValidationError(ERR_FIELD, ERR_DESCRIPTION);
        return Mono.just(new HashSet<>(Collections.singletonList(error)));
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        return Mono.just(entity.toBuilder()
                .withApplicationName(desiredAppName)
                .build());
    }
}
