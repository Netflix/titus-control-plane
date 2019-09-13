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

package com.netflix.titus.runtime.clustermembership.service;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.common.runtime.TitusRuntime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class ClusterMembershipServiceComponent {

    @Bean
    public ClusterMembershipServiceConfiguration getClusterMembershipServiceConfigurationBean(Environment environment) {
        return new ClusterMembershipServiceConfigurationBean(environment, "titus.clusterMembership");
    }

    @Bean
    public ClusterMembershipService getClusterMembershipService(ClusterMembershipServiceConfiguration configuration,
                                                                ClusterMembershipConnector connector,
                                                                HealthIndicator healthIndicator,
                                                                TitusRuntime titusRuntime) {
        return new DefaultClusterMembershipService(configuration, connector, healthIndicator, titusRuntime);
    }
}
