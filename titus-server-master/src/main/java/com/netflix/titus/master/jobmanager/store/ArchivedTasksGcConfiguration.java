/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.store;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.jobManager.archivedTasksGc")
public interface ArchivedTasksGcConfiguration {
    /**
     * @return whether or not the gc is enabled
     */
    @DefaultValue("true")
    boolean isGcEnabled();

    /**
     * @return the initial delay in milliseconds before the execution runs after process startup.
     */
    @DefaultValue("60000")
    long getGcInitialDelayMs();

    /**
     * @return the interval in milliseconds of how often the gc runs.
     */
    @DefaultValue("900000")
    long getGcIntervalMs();

    /**
     * @return the timeout of the gc's execution loop.
     */
    @DefaultValue("600000")
    long getGcTimeoutMs();

    /**
     * @return the the max number of archived tasks a job can have before tasks are gc'ed.
     */
    @DefaultValue("10000")
    long getMaxNumberOfArchivedTasksPerJob();

    /**
     * @return the the max number of archived tasks to GC per iteration.
     */
    @DefaultValue("10000")
    int getMaxNumberOfArchivedTasksToGcPerIteration();
}
