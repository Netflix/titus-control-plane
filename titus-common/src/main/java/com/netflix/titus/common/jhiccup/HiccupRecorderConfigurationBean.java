/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.jhiccup;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

@Singleton
public class HiccupRecorderConfigurationBean implements HiccupRecorderConfiguration {

    private final Environment environment;

    @Inject
    public HiccupRecorderConfigurationBean(Environment environment) {
        this.environment = environment;
    }

    @Override
    public long getReportingIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, "titus.jhiccup.reportingIntervalMs", 1000);
    }

    @Override
    public long getStartDelayMs() {
        return SpringConfigurationUtil.getLong(environment, "titus.jhiccup.startDelayMs", 1000);
    }

    @Override
    public long getTaskExecutionDeadlineMs() {
        return SpringConfigurationUtil.getLong(environment, "titus.jhiccup.taskExecutionDeadlineMs", 50);
    }
}
