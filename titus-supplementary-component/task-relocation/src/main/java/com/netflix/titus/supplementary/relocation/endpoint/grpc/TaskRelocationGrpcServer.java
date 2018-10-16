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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationEndpointConfiguration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TaskRelocationGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(TaskRelocationGrpcServer.class);

    private final TaskRelocationEndpointConfiguration configuration;
    private final TaskRelocationGrpcService taskRelocationGrpcService;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public TaskRelocationGrpcServer(TaskRelocationEndpointConfiguration configuration,
                                    TaskRelocationGrpcService taskRelocationGrpcService) {
        this.configuration = configuration;
        this.taskRelocationGrpcService = taskRelocationGrpcService;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = configure(ServerBuilder.forPort(configuration.getGrpcPort()));
            serverBuilder.addService(ServerInterceptors.intercept(
                    taskRelocationGrpcService,
                    createInterceptors(HealthGrpc.getServiceDescriptor())
            ));
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
            long timeoutMs = configuration.getShutdownTimeoutMs();
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
        return Arrays.asList(new ErrorCatchingServerInterceptor(), new V3HeaderInterceptor());
    }
}
