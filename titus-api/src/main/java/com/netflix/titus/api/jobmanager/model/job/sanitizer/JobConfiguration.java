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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.List;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Defines defaults/constraints/limits for job descriptor values.
 */
@Configuration(prefix = "titusMaster.job.configuration")
public interface JobConfiguration {

    long DEFAULT_RUNTIME_LIMIT_SEC = 432_000; // 5 days
    long MAX_RUNTIME_LIMIT_SEC = 864_000; // 10 days

    @DefaultValue("1000")
    int getMaxBatchJobSize();

    @DefaultValue("2500")
    int getMaxServiceJobSize();

    @DefaultValue("1.0")
    double getCpuMin();

    /**
     * An upper bound on CPUs a single container may allocate. The actual limit may be lower, as it also depends
     * on instance types available in a tier.
     */
    @DefaultValue("64")
    int getCpuMax();

    /**
     * An upper bound on GPUs a single container may allocate. The actual limit may be lower, as it also depends
     * on instance types available in a tier.
     */
    @DefaultValue("16")
    int getGpuMax();

    @DefaultValue("512")
    int getMemoryMbMin();

    /**
     * An upper bound on memory (megabytes) a single container may allocate. The actual limit may be lower, as it also depends
     * on instance types available in a tier.
     */
    @DefaultValue("524288")
    int getMemoryMbMax();

    @DefaultValue("10000")
    int getDiskMbMin();

    /**
     * An upper bound on disk (megabytes) a single container may allocate. The actual limit may be lower, as it also depends
     * on instance types available in a tier.
     */
    @DefaultValue("1000000")
    int getDiskMbMax();

    @DefaultValue("128")
    int getNetworkMbpsMin();

    /**
     * An upper bound on network (megabits per second) a single container may allocate. The actual limit may be lower, as it also depends
     * on instance types available in a tier.
     */
    @DefaultValue("40000")
    int getNetworkMbpsMax();

    @DefaultValue("" + DEFAULT_RUNTIME_LIMIT_SEC)
    long getDefaultRuntimeLimitSec();

    @DefaultValue("" + MAX_RUNTIME_LIMIT_SEC)
    long getMaxRuntimeLimitSec();

    /**
     * Default security group only set in V2 engine.
     */
    List<String> getDefaultSecurityGroups();

    /**
     * Default IAM profile only set in V2 engine.
     */
    @DefaultValue("")
    String getDefaultIamRole();
}
