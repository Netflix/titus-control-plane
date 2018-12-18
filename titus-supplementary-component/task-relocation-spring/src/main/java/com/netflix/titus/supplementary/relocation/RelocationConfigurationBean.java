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
import javax.inject.Singleton;

import com.netflix.titus.runtime.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

@Singleton
public class RelocationConfigurationBean implements RelocationConfiguration {

    private static final String PREFIX = "titus.relocation.";

    private final Environment environment;

    @Inject
    public RelocationConfigurationBean(Environment environment) {
        this.environment = environment;
    }

    @Override
    public long getRelocationScheduleIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "relocationScheduleIntervalMs", 30_000);
    }

    @Override
    public long getDeschedulingIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "deschedulingIntervalMs", 300_000);
    }

    @Override
    public long getRelocationTimeoutMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "relocationTimeoutMs", 300_000);
    }

    @Override
    public long getDataStalenessThresholdMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "dataStalenessThresholdMs", 30_000);
    }
}
