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

package com.netflix.titus.testkit.embedded.cloud.endpoint.grpc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.gateway.endpoint.v3.grpc.TitusGatewayGrpcServer;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceImplBase;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc.SimulatedMesosServiceImplBase;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloudConfiguration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SimulatedCloudGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(TitusGatewayGrpcServer.class);

    private final SimulatedCloudConfiguration configuration;
    private final SimulatedAgentServiceImplBase simulatedAgentsService;
    private final SimulatedMesosServiceImplBase simulatedMesosService;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public SimulatedCloudGrpcServer(SimulatedCloudConfiguration configuration,
                                    SimulatedAgentServiceImplBase simulatedAgentsService,
                                    SimulatedMesosServiceImplBase simulatedMesosService) {
        this.configuration = configuration;
        this.simulatedAgentsService = simulatedAgentsService;
        this.simulatedMesosService = simulatedMesosService;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = configure(ServerBuilder.forPort(configuration.getGrpcPort()));
            serverBuilder
                    .addService(ServerInterceptors.intercept(simulatedAgentsService, createInterceptors(SimulatedAgentServiceGrpc.getServiceDescriptor())))
                    .addService(ServerInterceptors.intercept(simulatedMesosService, createInterceptors(SimulatedMesosServiceGrpc.getServiceDescriptor())));
            this.server = serverBuilder.build();

            logger.info("Starting gRPC server on port {}.", configuration.getGrpcPort());
            try {
                this.server.start();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            logger.info("Started gRPC server on port {}.", configuration.getGrpcPort());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!server.isShutdown()) {
            server.shutdownNow();
        }
    }

    protected ServerBuilder configure(ServerBuilder serverBuilder) {
        return serverBuilder;
    }

    protected List<ServerInterceptor> createInterceptors(ServiceDescriptor serviceDescriptor) {
        return Collections.singletonList(new ErrorCatchingServerInterceptor());
    }
}
