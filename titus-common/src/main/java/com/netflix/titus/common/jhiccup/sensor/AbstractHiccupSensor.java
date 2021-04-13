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

package com.netflix.titus.common.jhiccup.sensor;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.jhiccup.HiccupRecorderConfiguration;

abstract class AbstractHiccupSensor implements HiccupSensor {

    /*
     * Store configuration in object, instead of calling HiccupRecorderConfiguration on each invocation
     * which may be expensive (especially for Spring Environment).
     */
    protected final long reportingIntervalMs;
    protected final long startDelayMs;
    protected final long taskExecutionDeadlineMs;

    protected final Registry registry;

    protected AbstractHiccupSensor(HiccupRecorderConfiguration configuration,
                                   Registry registry) {
        this.reportingIntervalMs = configuration.getReportingIntervalMs();
        this.startDelayMs = configuration.getStartDelayMs();
        this.taskExecutionDeadlineMs = configuration.getTaskExecutionDeadlineMs();
        this.registry = registry;
    }
}
