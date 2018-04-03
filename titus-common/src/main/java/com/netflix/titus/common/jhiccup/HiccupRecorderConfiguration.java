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

package com.netflix.titus.common.jhiccup;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.jhiccup")
public interface HiccupRecorderConfiguration {

    /**
     * Reporting interval.
     */
    @DefaultValue("1000")
    long getReportingIntervalMs();

    /**
     * An initial delay before reporting metrics.
     */
    @DefaultValue("1000")
    long getStartDelayMs();

    /**
     * Task execution deadline, which if crossed triggers additional reporting.
     */
    @DefaultValue("50")
    long getTaskExecutionDeadlineMs();
}
