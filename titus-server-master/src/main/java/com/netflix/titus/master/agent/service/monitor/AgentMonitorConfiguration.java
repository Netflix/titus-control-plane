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

package com.netflix.titus.master.agent.service.monitor;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.master.config.ConfigurationConstants;
import com.netflix.titus.master.config.ConfigurationConstants;

/**
 * Agent monitor configuration.
 */
@Configuration(prefix = "titus.agent.monitor")
public interface AgentMonitorConfiguration {

    /**
     * A period of time over which errors are counted. Errors older than this value
     * are ignored.
     *
     * @return time window in milliseconds
     */
    @DefaultValue(ConfigurationConstants.ONE_MINUTE)
    long getFailingAgentErrorCheckWindow();

    /**
     * Number of consecutive errors within an error window, that result in an agent isolation.
     *
     * @return number of consecutive errors
     */
    @DefaultValue("3")
    int getFailingAgentErrorCheckCount();

    @DefaultValue(ConfigurationConstants.ONE_MINUTE)
    long getHealthPollingInterval();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @DefaultValue("true")
    boolean isJobStatusMonitorEnabled();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @DefaultValue("true")
    boolean isHealthStatusMonitorEnabled();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @DefaultValue("true")
    boolean isAggregatingStatusMonitorEnabled();
}
