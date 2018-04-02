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

import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class ResourceLimitsValidator implements TitusJobSpecValidators.Validator {

    private final MasterConfiguration masterConfiguration;

    public ResourceLimitsValidator(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = masterConfiguration;
    }

    @Override
    public boolean isValid(TitusJobSpec titusJobSpec) {
        boolean cpuValid = titusJobSpec.getCpu() > 0 && titusJobSpec.getCpu() <= masterConfiguration.getMaxCPUs();
        boolean memoryValid = titusJobSpec.getMemory() > 0 && titusJobSpec.getMemory() <= masterConfiguration.getMaxMemory(); // r3.8xl limit
        boolean diskValid = titusJobSpec.getDisk() >= 0 && titusJobSpec.getDisk() <= masterConfiguration.getMaxDisk(); // r3.8xl limit
        boolean networkMbpsValid = titusJobSpec.getNetworkMbps() >= 0 && titusJobSpec.getNetworkMbps() <= masterConfiguration.getMaxNetworkMbps(); // r3.8xl limit
        return cpuValid && memoryValid && diskValid && networkMbpsValid;
    }
}
