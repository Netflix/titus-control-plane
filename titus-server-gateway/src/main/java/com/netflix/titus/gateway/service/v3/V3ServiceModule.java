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

package com.netflix.titus.gateway.service.v3;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.runtime.connector.agent.client.GrpcAgentManagementClient;
import com.netflix.titus.gateway.service.v3.internal.DefaultAutoScalingService;
import com.netflix.titus.gateway.service.v3.internal.DefaultHealthService;
import com.netflix.titus.gateway.service.v3.internal.GatewayJobManagementClient;
import com.netflix.titus.gateway.service.v3.internal.DefaultLoadBalancerService;
import com.netflix.titus.gateway.service.v3.internal.DefaultSchedulerService;
import com.netflix.titus.gateway.service.v3.internal.DefaultTitusManagementService;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import com.netflix.titus.runtime.service.AutoScalingService;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.service.HealthService;
import com.netflix.titus.runtime.service.LoadBalancerService;

public class V3ServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HealthService.class).to(DefaultHealthService.class);
        bind(JobManagementClient.class).to(GatewayJobManagementClient.class);
        bind(AutoScalingService.class).to(DefaultAutoScalingService.class);
        bind(LoadBalancerService.class).to(DefaultLoadBalancerService.class);
        bind(TitusManagementService.class).to(DefaultTitusManagementService.class);
        bind(AgentManagementClient.class).to(GrpcAgentManagementClient.class);
        bind(SchedulerService.class).to(DefaultSchedulerService.class);
    }

    @Provides
    @Singleton
    public GrpcClientConfiguration getGrpcClientConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcClientConfiguration.class);
    }

    @Provides
    @Singleton
    public JobManagerConfiguration getJobManagerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobManagerConfiguration.class);
    }
}
