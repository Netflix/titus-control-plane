/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.runtime.endpoint.admission;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.common.model.admission.AdmissionValidatorConfiguration;
import com.netflix.titus.common.model.admission.TitusValidatorConfiguration;

@Configuration(prefix = "titus.validate.job.security")
public interface JobSecurityValidatorConfiguration extends AdmissionValidatorConfiguration {

    @DefaultValue("true")
    boolean isIamValidatorEnabled();

    /**
     * If set to true, validation process executes assume role by calling AWS STS service. For safety reason, the
     * acquired credentials are valid for short amount of time only.
     */
    @DefaultValue("false")
    boolean isIamRoleWithStsValidationEnabled();

    @DefaultValue("titusagentInstanceProfile")
    String getAgentIamAssumeRole();

    /**
     * Since IAM validations are on the job accept path the timeout value is aggressive.
     * This must be smaller than {@link TitusValidatorConfiguration#getTimeoutMs()}.
     */
    @DefaultValue("4500")
    long getIamValidationTimeoutMs();

    @DefaultValue("1000,2000,3000")
    String getIamValidationHedgeThresholdsMs();
}
