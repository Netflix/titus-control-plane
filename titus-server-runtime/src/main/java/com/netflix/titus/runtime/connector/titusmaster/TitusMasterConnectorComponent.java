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

package com.netflix.titus.runtime.connector.titusmaster;

import javax.inject.Named;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class TitusMasterConnectorComponent {

    public static final String TITUS_MASTER_CHANNEL = "TitusMasterChannel";

    @Bean
    public TitusMasterClientConfiguration getTitusMasterClientConfiguration(Environment environment) {
        return new TitusMasterClientConfigurationBean(environment);
    }

    @Bean
    public SupervisorServiceGrpc.SupervisorServiceStub getSupervisorClientGrpcStub(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return SupervisorServiceGrpc.newStub(channel);
    }

    @Bean
    public AgentManagementServiceGrpc.AgentManagementServiceStub getAgentManagementClientGrpcStub(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return AgentManagementServiceGrpc.newStub(channel);
    }

    @Bean
    public JobManagementServiceGrpc.JobManagementServiceStub getJobManagementClientGrpcStub(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return JobManagementServiceGrpc.newStub(channel);
    }

    @Bean
    public EvictionServiceGrpc.EvictionServiceStub evictionClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return EvictionServiceGrpc.newStub(channel);
    }
}
