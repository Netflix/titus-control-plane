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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class LeaderActivationComponent {

    @Bean
    public LeaderActivationConfiguration getLeaderActivationConfiguration(TitusRuntime titusRuntime) {
        return Archaius2Ext.newConfiguration(LeaderActivationConfiguration.class, titusRuntime.getMyEnvironment());
    }
}
