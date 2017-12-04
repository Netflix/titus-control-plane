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

    @DefaultValue("60000")
    long getTaskInLaunchedStateTimeoutMs();

    @DefaultValue("720000")
    long getBatchTaskInStartInitiatedStateTimeoutMs();

    @DefaultValue("300000")
    long getServiceTaskInStartInitiatedStateTimeoutMs();

    @DefaultValue("600000")
    long getTaskInKillInitiatedStateTimeoutMs();

    @DefaultValue("12000")
    long getMaxActiveJobs();

    /**
     * @return the maximum number failed tasks to allow before exiting.
     */
    @DefaultValue("0")
    long getMaxFailedTasks();

    @DefaultValue("10000")
    long getTaskLivenessPollerIntervalMs();
}
