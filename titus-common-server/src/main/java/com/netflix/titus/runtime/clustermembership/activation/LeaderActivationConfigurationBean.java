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

    private final Environment environment;
    private final String prefix;

    public LeaderActivationConfigurationBean(Environment environment, String prefix) {
        this.environment = environment;
        this.prefix = prefix;
    }

    @Override
    public boolean isSystemExitOnLeadershipLost() {
        return SpringConfigurationUtil.getBoolean(environment, prefix + "systemExitOnLeadershipLost", false);
    }

    @Override
    public long getLeaderCheckIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "leaderCheckIntervalMs", 500);
    }

    @Override
    public long getLeaderActivationTimeout() {
        return SpringConfigurationUtil.getLong(environment, prefix + "leaderActivationTimeout", 180_000);
    }
}
