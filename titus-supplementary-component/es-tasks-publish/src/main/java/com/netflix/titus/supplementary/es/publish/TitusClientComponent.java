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
package com.netflix.titus.supplementary.es.publish;


import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
public class TitusClientComponent {
    private static final String GRPC_CLIENT_AGENT = "EsTaskPublisher";
    private static final int GRPC_KEEP_ALIVE_TIME = 5;
    private static final int GRPC_KEEP_ALIVE_TIMEOUT = 10;


    private final String titusApiHost;
    private final int titusApiPort;

    @Inject
    public TitusClientComponent(
            @Value("#{ @environment['titus.api.host'] ?: 'localhost' }") String titusApiHost,
            @Value("#{ @environment['titus.api.port'] ?: 7001 }") int titusApiPort) {
        this.titusApiHost = titusApiHost;
        this.titusApiPort = titusApiPort;
    }

//    @Lazy
//    @Bean
    public JobManagementServiceStub getJobManagementServiceStub() {
        return JobManagementServiceGrpc.newStub(getTitusGrpcChannel());
    }

    private ManagedChannel getTitusGrpcChannel() {
        return NettyChannelBuilder.forAddress(titusApiHost, titusApiPort)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .keepAliveTime(GRPC_KEEP_ALIVE_TIME, TimeUnit.SECONDS)
                .keepAliveTimeout(GRPC_KEEP_ALIVE_TIMEOUT, TimeUnit.SECONDS)
                .userAgent(GRPC_CLIENT_AGENT)
                .usePlaintext(true)
                .build();
    }
}
