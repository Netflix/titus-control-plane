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
package com.netflix.titus.es.publish;


import java.util.concurrent.TimeUnit;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TitusClientComponent {
    private static final String GRPC_CLIENT_AGENT = "EsTaskPublisher";
    private static final int GRPC_KEEP_ALIVE_TIME = 5;
    private static final int GRPC_KEEP_ALIVE_TIMEOUT = 10;
    public static final String TASK_DOCUMENT_CONTEXT = "taskDocumentContext";


    private final String titusApiHost;
    private final int titusApiPort;

    @Autowired
    public TitusClientComponent(
            @Value("#{ @environment['titus.api.host'] }") String titusApiHost,
            @Value("#{ @environment['titus.api.port'] }") int titusApiPort) {
        this.titusApiHost = titusApiHost;
        this.titusApiPort = titusApiPort;
    }

    @Bean
    public ManagedChannel getTitusGrpcChannel() {
        return NettyChannelBuilder.forAddress(titusApiHost, titusApiPort)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .keepAliveTime(GRPC_KEEP_ALIVE_TIME, TimeUnit.SECONDS)
                .keepAliveTimeout(GRPC_KEEP_ALIVE_TIMEOUT, TimeUnit.SECONDS)
                .userAgent(GRPC_CLIENT_AGENT)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public JobManagementServiceStub getJobManagementServiceStub() {
        return JobManagementServiceGrpc.newStub(getTitusGrpcChannel());
    }
}
