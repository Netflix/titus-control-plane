/*
 * Copyright 2020 Netflix, Inc.
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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({
        AggregatingCellClient.class,
        AggregatingHealthService.class,

        AggregatingJobManagementServiceHelper.class,
        AggregatingJobServiceGateway.class,
        AggregatingAutoScalingService.class,
        AggregatingLoadbalancerService.class,
        AggregatingReactorMachineServiceStub.class,
        DefaultAggregatingSchedulerService.class,
        DefaultJobActivityHistoryService.class,
        RemoteJobServiceGateway.class,
})

public class ServiceComponent {
    @Bean
    @Primary
    public JobServiceGateway getFallbackJobServiceGateway(
            TitusRuntime titusRuntime,
            TitusFederationConfiguration federationConfiguration,
            RemoteJobServiceGateway primary,
            AggregatingJobServiceGateway secondary) {
        return new FallbackJobServiceGateway(titusRuntime, federationConfiguration, primary, secondary);
    }
}
