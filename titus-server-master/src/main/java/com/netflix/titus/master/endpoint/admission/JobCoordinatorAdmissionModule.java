/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.endpoint.admission;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.runtime.endpoint.admission.PassJobValidator;

public class JobCoordinatorAdmissionModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public AdmissionSanitizer<JobDescriptor> getJobSanitizer() {
        return new PassJobValidator();
    }

    @Provides
    @Singleton
    public AdmissionValidator<JobDescriptor> getJobValidator() {
        return new PassJobValidator();
    }
}
