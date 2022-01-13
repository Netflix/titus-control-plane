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
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.elasticsearch.DefaultEsClient;
import com.netflix.titus.ext.elasticsearch.DefaultEsWebClientFactory;
import com.netflix.titus.ext.elasticsearch.EsClient;
import com.netflix.titus.ext.elasticsearch.EsClientConfiguration;
import com.netflix.titus.ext.elasticsearch.EsWebClientFactory;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceFutureStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.supplementary.taskspublisher.TaskDocument;
import com.netflix.titus.supplementary.taskspublisher.TaskEventsGenerator;
import com.netflix.titus.supplementary.taskspublisher.TitusClient;
import com.netflix.titus.supplementary.taskspublisher.TitusClientImpl;
import com.netflix.titus.supplementary.taskspublisher.es.EsPublisher;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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

    @Autowired
    private EsClientConfiguration esClientConfiguration;


    @Bean
    @ConditionalOnMissingBean
    public JobManagementServiceStub getJobManagementServiceStub() {
        return JobManagementServiceGrpc.newStub(getTitusGrpcChannel());
    }

    @Bean
    @ConditionalOnMissingBean
    public JobManagementServiceFutureStub getJobManagementServiceFutureStub() {
        return JobManagementServiceGrpc.newFutureStub(getTitusGrpcChannel());
    }

    @Bean
    @ConditionalOnMissingBean
    public TitusClient getTitusClient() {
        return new TitusClientImpl(getJobManagementServiceStub(), getJobManagementServiceFutureStub(), new DefaultRegistry());
    }

    @Bean
    @ConditionalOnMissingBean
    public EsClient<TaskDocument> getEsClient() {
        return new DefaultEsClient<>(getEsWebClientFactory());
    }

    @Bean
    @ConditionalOnMissingBean
    public EsWebClientFactory getEsWebClientFactory() {
        return new DefaultEsWebClientFactory(esClientConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskEventsGenerator getTaskEventsGenerator(TitusRuntime titusRuntime) {
        return new TaskEventsGenerator(getTitusClient(), Collections.emptyMap(), titusRuntime);
    }

    @Bean
    @ConditionalOnMissingBean
    public EsPublisher getEsPublisher(TitusRuntime titusRuntime) {
        return new EsPublisher(getTaskEventsGenerator(titusRuntime), getEsClient(), esPublisherConfiguration, titusRuntime.getRegistry());
    }


    private ManagedChannel getTitusGrpcChannel() {
        return NettyChannelBuilder.forAddress(titusApiHost, titusApiPort)
                .defaultLoadBalancingPolicy("round_robin")
                .keepAliveTime(GRPC_KEEP_ALIVE_TIME, TimeUnit.SECONDS)
                .keepAliveTimeout(GRPC_KEEP_ALIVE_TIMEOUT, TimeUnit.SECONDS)
                .userAgent(GRPC_CLIENT_AGENT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
    }
}
