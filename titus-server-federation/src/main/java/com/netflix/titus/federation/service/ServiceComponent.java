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

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        AggregatingCellClient.class,
        AggregatingHealthService.class,

        AggregatingJobServiceGateway.class,
        AggregatingJobManagementServiceHelper.class,
        AggregatingAutoScalingService.class,
        AggregatingLoadbalancerService.class,
        AggregatingReactorMachineServiceStub.class,
        DefaultAggregatingSchedulerService.class,
        DefaultJobActivityHistoryService.class,
})
public class ServiceComponent {
}
