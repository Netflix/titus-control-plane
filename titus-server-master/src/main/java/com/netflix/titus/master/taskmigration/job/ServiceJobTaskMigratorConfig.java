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

package com.netflix.titus.master.taskmigration.job;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.master.taskMigration")
public interface ServiceJobTaskMigratorConfig {

    /**
     * Default is true
     *
     * @return whether or not the service task migrator should run.
     */
    @DefaultValue("true")
    boolean isServiceTaskMigratorEnabled();

    /**
     * Default is 10 seconds
     *
     * @return the amount of time in milliseconds in between each scheduler iteration not including
     * the time taken to execute the code.
     */
    @DefaultValue("10000")
    long getSchedulerDelayMs();

    /**
     * Default is 5 minutes
     *
     * @return the amount of time in milliseconds the maximum time a scheduler iteration can take.
     */
    @DefaultValue("300000")
    long getSchedulerTimeoutMs();


    /**
     * @return the interval between task migration iterations.
     */
    @DefaultValue("900000")
    long getMigrateIntervalMs();

    /**
     * Default is 0.1%
     *
     * @return the percent of workers that should be migrated in a single iteration.
     */
    @DefaultValue("0.1")
    double getIterationPercent();

    /**
     * Default is 7 days
     *
     * @return the timeout in milliseconds that we should wait before migrating a self managed task
     */
    @DefaultValue("604800000")
    long getSelfManagedTimeoutMs();

    /**
     * Default is empty string
     *
     * @return a string of app names delimited by a comma
     */
    @DefaultValue("")
    String getAppNamesToIgnore();

    /*
     * @return the capacity of the terminate bucket
     */
    @DefaultValue("10")
    long getTerminateTokenBucketCapacity();

    /**
     * @return the per second refill rate of the terminate token bucket
     */
    @DefaultValue("10")
    long getTerminateTokenBucketRefillRatePerSecond();
}
