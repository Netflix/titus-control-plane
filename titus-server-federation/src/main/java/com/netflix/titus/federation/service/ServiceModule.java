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

package com.netflix.titus.federation.service;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import com.netflix.titus.runtime.service.AutoScalingService;
import com.netflix.titus.runtime.service.HealthService;
import com.netflix.titus.runtime.service.JobActivityHistoryService;
import com.netflix.titus.runtime.service.LoadBalancerService;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupClient;

public class ServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HealthService.class).to(AggregatingHealthService.class);
        bind(AggregatingSchedulerService.class).to(DefaultAggregatingSchedulerService.class);
        bind(JobServiceGateway.class).to(FallbackJobServiceGateway.class);
        bind(AutoScalingService.class).to(AggregatingAutoScalingService.class);
        bind(LoadBalancerService.class).to(AggregatingLoadbalancerService.class);
        bind(JobActivityHistoryService.class).to(NoopJobActivityHistoryService.class);
    }
}
