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

package com.netflix.titus.supplementary.relocation;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.relocation")
public interface RelocationConfiguration {

    /**
     * Interval at which the relocation workflow is triggered. This interval should be reasonably short, so the
     * relocation plans are up to date.
     */
    @DefaultValue("30000")
    long getRelocationScheduleIntervalMs();

    /**
     * Interval at which descheduling, and task eviction is executed. This interval must be aligned with
     * {@link #getRelocationScheduleIntervalMs()} interval, and should be a multiplication of the latter.
     */
    @DefaultValue("120000")
    long getDeschedulingIntervalMs();

    @DefaultValue("300000")
    long getRelocationTimeoutMs();

    @DefaultValue("30000")
    long getDataStalenessThresholdMs();

    @DefaultValue("90000")
    long getRdsTimeoutMs();

    @DefaultValue(".*")
    String getNodeRelocationRequiredTaints();

    @DefaultValue("NONE")
    String getNodeRelocationRequiredImmediatelyTaints();

    /**
     * Pattern identifying bad node conditions
     */
    @DefaultValue("UncorrectableMemoryFailure")
    String getBadNodeConditionPattern();

    /**
     * Pattern identifying bad node taints
     */
    @DefaultValue(".*unreachable")
    String getBadTaintsPattern();

    @DefaultValue("false")
    boolean isTaskTerminationOnBadNodeConditionEnabled();

    /**
     * Interval at which the node condition controller is triggered.
     */
    @DefaultValue("60000")
    long getNodeConditionControlLoopIntervalMs();

    /**
     * Node condition control loop timeout in Millis.
     */
    @DefaultValue("300000")
    long getNodeConditionControlLoopTimeoutMs();

    /**
     * It represents the last N seconds threshold for which the latest node condition is sustained
     * It helps us avoid picking up node conditions that are reached for a very short duration
     */
    @DefaultValue("300")
    int getNodeConditionTransitionTimeThresholdSeconds();

    /**
     * It represents the last N seconds threshold for which the latest taint is sustained
     * It helps us avoid picking up taint state that is sustained for a very short duration
     */
    @DefaultValue("300")
    int getNodeTaintTransitionTimeThresholdSeconds();

}
