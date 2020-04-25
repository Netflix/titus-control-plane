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

import java.util.function.UnaryOperator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import reactor.core.publisher.Mono;

/**
 * This {@link AdmissionSanitizer} implementation ensures a job's appname matches a specific string.
 * It is only used for testing purposes.
 */
class TestingAppNameSanitizer implements AdmissionSanitizer<JobDescriptor> {
    private final String desiredAppName;

    TestingAppNameSanitizer() {
        this("desiredAppName");
    }

    TestingAppNameSanitizer(String desiredAppName) {
        this.desiredAppName = desiredAppName;
    }

    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor entity) {
        return Mono.just(jd -> entity.toBuilder().withApplicationName(desiredAppName).build());
    }

    public String getDesiredAppName() {
        return desiredAppName;
    }
}
