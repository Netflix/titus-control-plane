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

package com.netflix.titus.master.endpoint.v2.validator;


import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.master.RuntimeLimitValidator;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class TitusJobSpecValidators {
    private List<TitusJobSpecValidators.Validator> validators = new ArrayList<>(5);

    public interface Validator {
        boolean isValid(TitusJobSpec titusJobSpec);
    }

    public class ValidationResult {
        public final boolean isValid;
        public final List<TitusJobSpecValidators.Validator> failedValidators;


        public ValidationResult(boolean isValid, List<Validator> failedValidators) {
            this.isValid = isValid;
            this.failedValidators = failedValidators;
        }
    }

    public TitusJobSpecValidators(MasterConfiguration masterConfiguration,
                                  JobConfiguration jobConfiguration,
                                  ValidatorConfiguration validatorConfiguration) {
        this.validators.add(new ResourceLimitsValidator(masterConfiguration));
        this.validators.add(new InstanceCountValidator(masterConfiguration));
        this.validators.add(new DockerImageValidator());
        this.validators.add(new EfsValidator(validatorConfiguration));
        this.validators.add(new RuntimeLimitValidator(jobConfiguration));
    }

    public ValidationResult validate(TitusJobSpec jobSpec) {
        List<TitusJobSpecValidators.Validator> failedValidators = new ArrayList<>(5);
        boolean isValid = true;
        for (TitusJobSpecValidators.Validator validator : validators) {
            if (!validator.isValid(jobSpec)) {
                failedValidators.add(validator);
                isValid = false;
            }
        }
        return new ValidationResult(isValid, failedValidators);
    }
}
