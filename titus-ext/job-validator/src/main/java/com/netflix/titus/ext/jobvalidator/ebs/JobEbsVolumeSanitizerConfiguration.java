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

package com.netflix.titus.ext.jobvalidator.ebs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.common.model.admission.AdmissionValidatorConfiguration;
import com.netflix.titus.common.model.admission.TitusValidatorConfiguration;

@Configuration(prefix = "titus.validate.job.ebs")
public interface JobEbsVolumeSanitizerConfiguration extends AdmissionValidatorConfiguration {
    @DefaultValue("true")
    boolean isEnabled();

    /**
     * The aggregate timeout for the validation of all EBS volumes for a job. Since EBS volume
     * validation is required to get correct EBS metadata and there may be multiple volumes for
     * a job we want to provide sufficient time. However, since sanitization is on the job accept
     * path the timeout should not be too large.
     * This must be smaller than {@link TitusValidatorConfiguration#getTimeoutMs()} to be effective.
     */
    @DefaultValue("4500")
    long getJobEbsSanitizationTimeoutMs();
}
