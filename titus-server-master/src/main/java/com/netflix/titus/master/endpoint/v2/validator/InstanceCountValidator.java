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

import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class InstanceCountValidator implements TitusJobSpecValidators.Validator {
    private final MasterConfiguration masterConfiguration;

    public InstanceCountValidator(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = masterConfiguration;
    }

    public boolean isValid(TitusJobSpec titusJobSpec) {
        final TitusJobType jobType = titusJobSpec.getType();
        final int batchInstances = titusJobSpec.getInstances();
        final int serviceMin = titusJobSpec.getInstancesMin();
        final int serviceMax = titusJobSpec.getInstancesMax();
        final int serviceDesired = titusJobSpec.getInstancesDesired();

        if (jobType == TitusJobType.batch) {
            return batchInstances > 0 && batchInstances <= masterConfiguration.getMaxBatchInstances();
        } else {
            return serviceMin >= 0 &&
                    serviceDesired >= serviceMin &&
                    serviceMax >= serviceDesired &&
                    serviceMin <= masterConfiguration.getMaxServiceInstances() &&
                    serviceDesired <= masterConfiguration.getMaxServiceInstances() &&
                    serviceMax <= masterConfiguration.getMaxServiceInstances();
        }
    }
}
