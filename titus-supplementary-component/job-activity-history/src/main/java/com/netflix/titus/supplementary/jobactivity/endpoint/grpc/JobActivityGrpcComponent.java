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

package com.netflix.titus.supplementary.jobactivity.endpoint.grpc;

import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfigurationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class JobActivityGrpcComponent {

    @Bean
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(Environment environment) {
        return new GrpcEndpointConfigurationBean(environment, "titus.jobactivity.endpoint.");
    }

    @Bean
    public JobActivityGrpcService getJobActivityGrpcService() {
        return new JobActivityGrpcService();
    }
}
