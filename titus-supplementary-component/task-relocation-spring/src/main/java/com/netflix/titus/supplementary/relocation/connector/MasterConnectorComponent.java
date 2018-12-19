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

package com.netflix.titus.supplementary.relocation.connector;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.client.GrpcAgentManagementClient;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.client.GrpcJobManagementClient;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class MasterConnectorComponent {

    @Bean
    public GrpcClientConfiguration getGrpcClientConfiguration(Environment environment) {
        return new GrpcClientConfigurationBean(environment);
    }

    @Bean
    public AgentManagementClient getAgentManagementClient(GrpcClientConfiguration configuration,
                                                          AgentManagementServiceStub clientStub,
                                                          CallMetadataResolver callMetadataResolver) {
        return new GrpcAgentManagementClient(configuration, clientStub, callMetadataResolver);
    }

    @Bean
    public JobManagementClient getJobManagementClient(GrpcClientConfiguration configuration,
                                                      JobManagementServiceStub clientStub,
                                                      CallMetadataResolver callMetadataResolver) {
        return new GrpcJobManagementClient(clientStub, callMetadataResolver, configuration);
    }

    @Bean
    public EvictionServiceClient getEvictionServiceClient(ReactorGrpcClientAdapterFactory grpcClientAdapterFactory,
                                                          EvictionServiceStub clientStub) {
        return new GrpcEvictionServiceClient(grpcClientAdapterFactory, clientStub);
    }
}
