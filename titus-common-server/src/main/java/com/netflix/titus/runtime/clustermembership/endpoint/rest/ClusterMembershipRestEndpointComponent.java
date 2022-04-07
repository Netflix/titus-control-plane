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

package com.netflix.titus.runtime.clustermembership.endpoint.rest;

import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationStatus;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.GrpcClusterMembershipService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ClusterMembershipRestEndpointComponent implements WebMvcConfigurer {

    @Autowired
    private LeaderActivationStatus leaderActivationStatus;

    @Bean
    public ClusterMembershipSpringResource getClusterMembershipSpringResource(GrpcClusterMembershipService reactorClusterMembershipService) {
        return new ClusterMembershipSpringResource(reactorClusterMembershipService);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(SpringLeaderServerInterceptor.clusterMembershipAllowed(leaderActivationStatus));
    }
}
