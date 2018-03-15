/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 */
@Configuration(prefix = "titusMaster.jobManager")
public interface JobManagerConfiguration {

    @DefaultValue("1000")
    long getReconcilerIdleTimeoutMs();

    @DefaultValue("50")
    long getReconcilerActiveTimeoutMs();

    /**
     * How many active tasks in the transient state (in other words not Started and not Finished) are allowed in a job.
     * If the number of active tasks in the transient state goes above this limit, no new tasks are created.
     */
    @DefaultValue("100")
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
     * @return whether or not the ignoreLaunchGuard flag should be sent to the agent on a v3 task.
     */
    @DefaultValue("false")
    boolean isV3IgnoreLaunchGuardEnabled();

    /**
     * Feature flag controlling job/task validation process.
     */
    @DefaultValue("false")
    boolean isFailOnDataValidation();

    /**
     * @return whether or not the nested containers should be allowed.
     */
    @DefaultValue("false")
    boolean isNestedContainersEnabled();

    /**
     * @return the min that can be set on the killWaitSeconds field. The default value will be used instead if the value specified
     * is lower than the min.
     */
    @DefaultValue("10")
    int getMinKillWaitSeconds();

    /**
     * @return the max that can be set on the killWaitSeconds field. The default value will be used instead if the value specified
     * is greater than the max.
     */
    @DefaultValue("300")
    int getMaxKillWaitSeconds();

    /**
     * @return maximum amount of seconds to wait before forcefully terminating a container.
     */
    @DefaultValue("10")
    int getDefaultKillWaitSeconds();

    /**
     * @return Alternative location to fetch executor from. If set, the master will populate the command info to use this URI
     * as opposed to the on-host executor
     */
    @DefaultValue("")
    String getExecutorUri();
}
