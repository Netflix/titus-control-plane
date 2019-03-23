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
package com.netflix.titus.supplementary.taskspublisher.config;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.supplementary.taskspublisher.DefaultEsWebClientFactory;
import com.netflix.titus.supplementary.taskspublisher.EsClient;
import com.netflix.titus.supplementary.taskspublisher.EsClientHttp;
import com.netflix.titus.supplementary.taskspublisher.EsWebClientFactory;
import com.netflix.titus.supplementary.taskspublisher.TasksPublisherCtrl;
import com.netflix.titus.supplementary.taskspublisher.TitusClient;
import com.netflix.titus.supplementary.taskspublisher.TitusClientImpl;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class TasksPublisherConfiguration {
    private static final String GRPC_CLIENT_AGENT = "EsTaskPublisher";
    private static final int GRPC_KEEP_ALIVE_TIME = 5;
    private static final int GRPC_KEEP_ALIVE_TIMEOUT = 10;


    @Autowired
    @Value("${titus.api.host: 'localhost'}")
    private String titusApiHost;

    @Autowired
    @Value("${titus.api.port: 7001}")
    private int titusApiPort;


    @Autowired
    private EsPublisherConfiguration esPublisherConfiguration;


    @Bean
    @ConditionalOnMissingBean
    public JobManagementServiceStub getJobManagementServiceStub() {
        return JobManagementServiceGrpc.newStub(getTitusGrpcChannel());
    }


    @Bean
    public TitusClient getTitusClient() {
        return new TitusClientImpl(getJobManagementServiceStub(), new DefaultRegistry());
    }

    @Bean
    public EsClient getEsClient() {
        return new EsClientHttp(esPublisherConfiguration, getEsWebClientFactory());
    }

    @Bean
    public EsWebClientFactory getEsWebClientFactory() {
        return new DefaultEsWebClientFactory(esPublisherConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean
    public TasksPublisherCtrl getTasksPublisherCtrl() {
        return new TasksPublisherCtrl(getEsClient(), getTitusClient(), Collections.emptyMap(), new DefaultRegistry());
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
