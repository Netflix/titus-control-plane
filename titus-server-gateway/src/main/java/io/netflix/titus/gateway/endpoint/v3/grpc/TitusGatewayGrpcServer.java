/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.gateway.endpoint.v3.grpc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import io.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class TitusGatewayGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(TitusGatewayGrpcServer.class);

    private final JobManagementServiceImplBase jobManagementService;
    private final AgentManagementServiceImplBase agentManagementService;
    private final AutoScalingServiceGrpc.AutoScalingServiceImplBase appAutoScalingService;
    private final io.netflix.titus.gateway.endpoint.v3.grpc.GrpcEndpointConfiguration config;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public TitusGatewayGrpcServer(
            JobManagementServiceImplBase jobManagementService,
            AgentManagementServiceImplBase agentManagementService,
            AutoScalingServiceGrpc.AutoScalingServiceImplBase appAutoScalingService,
            GrpcEndpointConfiguration config) {
        this.jobManagementService = jobManagementService;
        this.agentManagementService = agentManagementService;
        this.appAutoScalingService = appAutoScalingService;
        this.config = config;
    }

    @PostConstruct
    public void start() throws Exception {
        if (!started.getAndSet(true)) {
            this.server = configure(ServerBuilder.forPort(config.getPort()))
                    .addService(ServerInterceptors.intercept(
                            jobManagementService,
                            createInterceptors(JobManagementServiceGrpc.getServiceDescriptor())
                    ))
                    .addService(ServerInterceptors.intercept(
                            agentManagementService,
                            createInterceptors(AgentManagementServiceGrpc.getServiceDescriptor())
                    ))
                    .addService(ServerInterceptors.intercept(
                            appAutoScalingService,
                            createInterceptors(AutoScalingServiceGrpc.getServiceDescriptor())
                    ))
                    .build();

            LOG.info("Starting gRPC server on port {}.", config.getPort());
            try {
                this.server.start();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Started gRPC server on port {}.", config.getPort());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!server.isShutdown()) {
            long timeoutMs = config.getShutdownTimeoutMs();
            if (timeoutMs <= 0) {
                server.shutdownNow();
            } else {
                server.shutdown();
                try {
                    server.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignore) {
                }
                if (!server.isShutdown()) {
                    server.shutdownNow();
                }
            }
        }
    }

    /**
     * Override to change default server configuration.
     */
    protected ServerBuilder configure(ServerBuilder serverBuilder) {
        return serverBuilder;
    }

    /**
     * Override to add server side interceptors.
     */
    protected List<ServerInterceptor> createInterceptors(ServiceDescriptor serviceDescriptor) {
        return Collections.singletonList(new ErrorCatchingServerInterceptor());
    }
}
