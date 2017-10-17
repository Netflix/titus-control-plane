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

package io.netflix.titus.master.agent.service.monitor;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import io.netflix.titus.master.config.ConfigurationConstants;

/**
 * Agent monitor configuration.
 */
public interface AgentMonitorConfiguration {

    /**
     * Amount of time after which if no event was received for a particular agent, it is assumed
     * to be dead, and all its monitoring state can be released.
     *
     * @return timeout value in milliseconds
     */
    @PropertyName(name = "mantis.agent.monitor.deadAgentTimeout")
    @DefaultValue(ConfigurationConstants.ONE_HOUR)
    long getDeadAgentTimeout();

    /**
     * Isolation time for agent which jobs are failing constantly.
     *
     * @return isolation time in milliseconds
     */
    @PropertyName(name = "mantis.agent.monitor.failingAgentIsolationTime")
    @DefaultValue(ConfigurationConstants.ONE_MINUTE)
    long getFailingAgentIsolationTime();

    /**
     * A period of time over which errors are counted. Errors older than this value
     * are ignored.
     *
     * @return time window in milliseconds
     */
    @PropertyName(name = "mantis.agent.monitor.failingAgentErrorCheckWindow")
    @DefaultValue(ConfigurationConstants.ONE_MINUTE)
    long getFailingAgentErrorCheckWindow();

    /**
     * Number of consecutive errors within an error window, that result in an agent isolation.
     *
     * @return number of consecutive errors
     */
    @PropertyName(name = "mantis.agent.monitor.failingAgentErrorCheckCount")
    @DefaultValue("3")
    int getFailingAgentErrorCheckCount();

    @PropertyName(name = "mantis.agent.monitor.healthPollingInterval")
    @DefaultValue(ConfigurationConstants.ONE_MINUTE)
    long getHealthPollingInterval();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @PropertyName(name = "mantis.agent.monitor.jobStatusMonitorEnabled")
    @DefaultValue("true")
    boolean isJobStatusMonitorEnabled();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @PropertyName(name = "mantis.agent.monitor.healthStatusMonitorEnabled")
    @DefaultValue("true")
    boolean isHealthStatusMonitorEnabled();

    /**
     * To disable {@link V2JobStatusMonitor} set this flag to false.
     */
    @PropertyName(name = "mantis.agent.monitor.aggregatingStatusMonitorEnabled")
    @DefaultValue("true")
    boolean isAggregatingStatusMonitorEnabled();

    /**
     * A percentage of agents that can be disabled. If a number of agents in unhealthy state exceed this limit,
     * only the configured percentage will be disabled, while other will be force-set to state 'healthy'.
     */
    @PropertyName(name = "mantis.agent.monitor.disableAgentPercentageThreshold")
    @DefaultValue("20")
    int getDisableAgentPercentageThreshold();
}
