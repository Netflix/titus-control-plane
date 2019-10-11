/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.clustermembership.activation;

import com.netflix.titus.common.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

public class LeaderActivationConfigurationBean implements LeaderActivationConfiguration {

    public static final String PREFIX = "titus.leaderActivation.controller.";

    private final Environment environment;

    public LeaderActivationConfigurationBean(Environment environment) {
        this.environment = environment;
    }

    @Override
    public boolean isSystemExitOnLeadershipLost() {
        return SpringConfigurationUtil.getBoolean(environment, PREFIX + "systemExitOnLeadershipLost", false);
    }

    @Override
    public long getLeaderCheckIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "leaderCheckIntervalMs", 500);
    }

    @Override
    public long getLeaderActivationTimeout() {
        return SpringConfigurationUtil.getLong(environment, PREFIX + "leaderActivationTimeout", 180_000);
    }
}
