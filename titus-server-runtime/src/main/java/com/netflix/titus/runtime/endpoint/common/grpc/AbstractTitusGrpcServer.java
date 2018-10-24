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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTitusGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTitusGrpcServer.class);

    private final GrpcEndpointConfiguration configuration;
    private final BindableService bindableService;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    /**
     * If the configured port is 0, the assigned ephemeral port will be set here after the server is started.
     */
    private int port;

    protected AbstractTitusGrpcServer(GrpcEndpointConfiguration configuration,
                                      BindableService bindableService) {
        this.configuration = configuration;
        this.bindableService = bindableService;
    }

    public int getPort() {
        return port;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = configure(ServerBuilder.forPort(configuration.getPort()));
            serverBuilder.addService(ServerInterceptors.intercept(
                    bindableService,
                    createInterceptors(HealthGrpc.getServiceDescriptor())
            ));
            this.server = serverBuilder.build();

            logger.info("Starting {} on port {}.", getClass().getSimpleName(), configuration.getPort());
            try {
                this.server.start();
                this.port = server.getPort();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            logger.info("Started {} on port {}.", getClass().getSimpleName(), port);
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
