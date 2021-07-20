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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobManagerConnectorComponent {

    public static final String JOB_MANAGER_CHANNEL = "jobManagerChannel";

    @Bean
    public JobManagementClient getJobManagementClient(ReactorJobManagementServiceStub stub, TitusRuntime titusRuntime) {
        return new RemoteJobManagementClient("extClient", stub, titusRuntime);
    }

    @Bean
    public ReactorJobManagementServiceStub getReactorJobManagementServiceStub(GrpcToReactorClientFactory factory,
                                                                              @Named(JOB_MANAGER_CHANNEL) Channel channel) {
        return factory.apply(JobManagementServiceGrpc.newStub(channel), ReactorJobManagementServiceStub.class, JobManagementServiceGrpc.getServiceDescriptor());
    }
}
