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

package com.netflix.titus.supplementary.relocation;

import javax.inject.Inject;

import com.netflix.titus.common.util.SpringConfigurationUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class RelocationConfiguration {

    private static final String PREFIX = "titus.relocation.";

    private final Environment environment;

    @Value("${titus.relocation.relocationScheduleIntervalMs:30000}")
    private long relocationScheduleIntervalMs;

    @Inject
    public RelocationConfiguration(Environment environment) {
        this.environment = environment;
    }

    /**
     * Interval at which the relocation workflow is triggered. This interval should be reasonably short, so the
     * relocation plans are up to date.
     */
    public long getRelocationScheduleIntervalMs() {
        return relocationScheduleIntervalMs;
    }

    /**
     * Interval at which descheduling, and task eviction is executed. This interval must be aligned with
     * {@link #getRelocationScheduleIntervalMs()} interval, and should be a multiplication of the latter.
     */
    public long getDeschedulingIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "deschedulingIntervalMs", 120_000);
    }

    public long getRelocationTimeoutMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "relocationTimeoutMs", 300_000);
    }

    public long getDataStalenessThresholdMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "dataStalenessThresholdMs", 30_000);
    }
}
