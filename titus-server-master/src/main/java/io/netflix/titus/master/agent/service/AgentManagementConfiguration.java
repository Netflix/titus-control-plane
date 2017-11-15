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

package io.netflix.titus.master.agent.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.agent")
public interface AgentManagementConfiguration {

    @DefaultValue("60000")
    long getCacheRefreshIntervalMs();

    @DefaultValue("120000")
    long getFullCacheRefreshIntervalMs();

    @DefaultValue(".*")
    String getAgentInstanceGroupPattern();

    @DefaultValue("2")
    int getAutoScaleRuleMinIdleToKeep();

    @DefaultValue("5")
    int getAutoScaleRuleMaxIdleToKeep();

    @DefaultValue("0")
    int getAutoScaleRuleMin();

    @DefaultValue("1000")
    int getAutoScaleRuleMax();

    @DefaultValue("600")
    int getAutoScaleRuleCoolDownSec();

    @DefaultValue("1")
    int getAutoScaleRuleShortfallAdjustingFactor();
}
