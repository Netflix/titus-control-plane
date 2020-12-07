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
     * Set to true to enable routing GPU jobs to KubeScheduler.
     */
    @DefaultValue("false")
    boolean isGpuEnabled();

    /**
     * Enable container size limit check. Containers large than the limit are not be assigned to KubeScheduler.
     */
    @DefaultValue("true")
    boolean isContainerSizeLimitEnabled();

    /**
     * Maximum CPU allocation managed by KubeScheduler.
     */
    @DefaultValue("16")
    int getCpuLimit();

    /**
     * Maximum GPU allocation managed by KubeScheduler.
     */
    @DefaultValue("1")
    int getGpuLimit();

    /**
     * Maximum memory allocation managed by KubeScheduler.
     */
    @DefaultValue("131072")
    int getMemoryMBLimit();

    /**
     * Maximum disk allocation managed by KubeScheduler.
     */
    @DefaultValue("262144")
    int getDiskMBLimit();

    /**
     * Maximum network allocation managed by KubeScheduler.
     */
    @DefaultValue("4000")
    int getNetworkMbpsLimit();
}
