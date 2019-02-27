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

package com.netflix.titus.runtime.connector.jobmanager;

import javax.inject.Named;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.runtime.connector.ChannelTunablesConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.client.GrpcJobManagementClient;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobManagerConnectorComponent {

    public static final String JOB_MANAGER_CHANNEL = "jobManagerChannel";

    @Bean
    public JobManagementServiceStub getJobManagementClientGrpcStub(final @Named(JOB_MANAGER_CHANNEL) Channel channel) {
        return JobManagementServiceGrpc.newStub(channel);
    }


    @Bean
    public JobManagementClient getJobManagementClient(ChannelTunablesConfiguration configuration,
                                                      JobManagementServiceStub clientStub,
                                                      CallMetadataResolver callMetadataResolver) {
        return new GrpcJobManagementClient(clientStub, callMetadataResolver, configuration);
    }
}
