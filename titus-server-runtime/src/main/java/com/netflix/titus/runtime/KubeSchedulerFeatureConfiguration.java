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

package com.netflix.titus.runtime;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.features.jobManager.kubeSchedulerFeature")
public interface KubeSchedulerFeatureConfiguration {

    /**
     * Allows tier level feature gate for routing Static IP jobs. Set to .* to route these jobs for both tiers.
     */
    @DefaultValue("")
    String getEnabledStaticIpOverrideTiers();

    /**
     * Allows tier level feature gate for routing EBS Volume jobs. Set to .* to route these jobs for both tiers.
     */
    @DefaultValue("")
    String getEnabledEbsVolumeOverrideTiers();

    /**
     * Jobs with machine type hard constraint which request machine types not allowed by this regexp pattern, will be
     * assigned to Fenzo.
     */
    @DefaultValue(".*")
    String getEnabledMachineTypes();
}
