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

package com.netflix.titus.runtime.connector;

import java.time.Duration;

import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.runtime.connector.common.reactor.ReactorToGrpcClientBuilder;
import com.netflix.titus.runtime.connector.common.reactor.ReactorToGrpcClientFactory;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.client.GrpcJobManagementClient;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.AbstractStub;
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
    public ReactorToGrpcClientFactory getReactorGrpcClientAdapterFactory(GrpcClientConfiguration configuration,
                                                                         CallMetadataResolver callMetadataResolver) {
        return new ReactorToGrpcClientFactory() {
            @Override
            public <GRPC_STUB extends AbstractStub<GRPC_STUB>, REACT_API> REACT_API apply(GRPC_STUB stub, Class<REACT_API> apiType) {
                return ReactorToGrpcClientBuilder
                        .newBuilder(
                                apiType, stub, TaskRelocationServiceGrpc.getServiceDescriptor()
                        )
                        .withCallMetadataResolver(callMetadataResolver)
                        .withTimeout(Duration.ofMillis(configuration.getRequestTimeout()))
                        .build();
            }
        };
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
