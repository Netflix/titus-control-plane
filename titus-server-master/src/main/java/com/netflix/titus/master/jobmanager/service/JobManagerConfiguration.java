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

package com.netflix.titus.master.jobmanager.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 */
@Configuration(prefix = "titusMaster.jobManager")
public interface JobManagerConfiguration {

    @DefaultValue("100")
    long getReconcilerIdleTimeoutMs();

    @DefaultValue("1")
    long getReconcilerActiveTimeoutMs();

    /**
     * How many active tasks in the transient state (in other words not Started and not Finished) are allowed in a job.
     * If the number of active tasks in the transient state goes above this limit, no new tasks are created.
     */
    @DefaultValue("300")
    int getActiveNotStartedTasksLimit();

    @DefaultValue("60000")
    long getTaskInLaunchedStateTimeoutMs();

    @DefaultValue("720000")
    long getBatchTaskInStartInitiatedStateTimeoutMs();

    @DefaultValue("300000")
    long getServiceTaskInStartInitiatedStateTimeoutMs();

    @DefaultValue("600000")
    long getTaskInKillInitiatedStateTimeoutMs();

    /**
     * Minimum amount of time a task should stay in Started state, before the retryer that is associated with it is
     * restarted.
     */
    @DefaultValue("300000")
    long getTaskRetryerResetTimeMs();

    /**
     * A lower bound on the retry interval.
     */
    @DefaultValue("1000")
    long getMinRetryIntervalMs();

    @DefaultValue("2")
    long getTaskKillAttempts();

    @DefaultValue("12000")
    long getMaxActiveJobs();

    /**
     * @return the maximum allowed number of jobs that could not be loaded from the database (corrupted records, inconsistent data, etc)
     */
    @DefaultValue("0")
    long getMaxFailedJobs();

    /**
     * @return the maximum allowed number of tasks that could not be loaded from the database (corrupted records, inconsistent data, etc)
     */
    @DefaultValue("0")
    long getMaxFailedTasks();

    @DefaultValue("10000")
    long getTaskLivenessPollerIntervalMs();

    /**
     * Feature flag controlling job/task validation process.
     */
    @DefaultValue("false")
    boolean isFailOnDataValidation();

    /**
     * Feature flag disabling movetask api
     */
    @DefaultValue("false")
    boolean enableMoveTaskApi();
}
