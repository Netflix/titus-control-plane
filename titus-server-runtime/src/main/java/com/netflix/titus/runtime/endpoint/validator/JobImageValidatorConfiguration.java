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

package com.netflix.titus.runtime.endpoint.validator;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.common.model.validator.EntityValidatorConfiguration;

@Configuration(prefix = "titus.validate.job.image")
public interface JobImageValidatorConfiguration extends EntityValidatorConfiguration {
    @DefaultValue("true")
    boolean isEnabled();

    /**
     * Since Image validations are on the job accept path the timeout value is aggressive.
     * This must be smaller than {@link TitusValidatorConfiguration#getTimeoutMs()}.
     */
    @DefaultValue("1200")
    long getJobImageValidationTimeoutMs();
}
