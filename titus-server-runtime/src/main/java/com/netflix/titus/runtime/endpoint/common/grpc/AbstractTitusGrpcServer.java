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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTitusGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTitusGrpcServer.class);

    private final GrpcEndpointConfiguration configuration;
    private final ServerServiceDefinition serviceDefinition;
    private final TitusRuntime runtime;
    private final ExecutorService grpcCallbackExecutor;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    /**
     * If the configured port is 0, the assigned ephemeral port will be set here after the server is started.
     */
    private int port;

    protected AbstractTitusGrpcServer(GrpcEndpointConfiguration configuration,
                                      BindableService bindableService,
                                      TitusRuntime runtime) {
        this.configuration = configuration;
        this.serviceDefinition = bindableService.bindService();
        this.runtime = runtime;
        this.grpcCallbackExecutor = ExecutorsExt.instrumentedCachedThreadPool(runtime.getRegistry(), "grpcCallbackExecutor");
    }

    protected AbstractTitusGrpcServer(GrpcEndpointConfiguration configuration,
                                      ServerServiceDefinition serviceDefinition, TitusRuntime runtime) {
        this.configuration = configuration;
        this.serviceDefinition = serviceDefinition;
        this.runtime = runtime;
        this.grpcCallbackExecutor = ExecutorsExt.instrumentedCachedThreadPool(runtime.getRegistry(), "grpcCallbackExecutor");
    }

    public int getPort() {
        return port;
    }

    @PostConstruct
    public void start() {
        if (started.getAndSet(true)) {
            return;
        }
        this.server = configure(ServerBuilder.forPort(port).executor(grpcCallbackExecutor))
                .addService(ServerInterceptors.intercept(
                        serviceDefinition,
                        createInterceptors(HealthGrpc.getServiceDescriptor())
                ))
                .build();

        logger.info("Starting {} on port {}.", getClass().getSimpleName(), configuration.getPort());
        try {
            this.server.start();
            this.port = server.getPort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Started {} on port {}.", getClass().getSimpleName(), port);
    }

    @PreDestroy
    public void shutdown() {
        if (server.isShutdown()) {
            return;
        }
        long timeoutMs = configuration.getShutdownTimeoutMs();
        try {
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
        } finally {
            grpcCallbackExecutor.shutdown();
            if (timeoutMs > 0) {
                try {
                    grpcCallbackExecutor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignore) {
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
