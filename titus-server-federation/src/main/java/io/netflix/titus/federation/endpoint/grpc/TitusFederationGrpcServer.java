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

package io.netflix.titus.federation.endpoint.grpc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import io.netflix.titus.federation.endpoint.EndpointConfiguration;
import io.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class TitusFederationGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(TitusFederationGrpcServer.class);

    private final JobManagementServiceImplBase jobManagementService;
    private AutoScalingServiceImplBase autoScalingService;
    private final EndpointConfiguration config;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public TitusFederationGrpcServer(
            JobManagementServiceImplBase jobManagementService,
            AutoScalingServiceImplBase autoScalingService,
            EndpointConfiguration config) {
        this.jobManagementService = jobManagementService;
        this.autoScalingService = autoScalingService;
        this.config = config;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = configure(ServerBuilder.forPort(config.getGrpcPort()));
            serverBuilder.addService(ServerInterceptors.intercept(
                    jobManagementService,
                    createInterceptors(JobManagementServiceGrpc.getServiceDescriptor())
            ));
            serverBuilder.addService(ServerInterceptors.intercept(
                    autoScalingService,
                    createInterceptors(AutoScalingServiceGrpc.getServiceDescriptor())
            ));

            this.server = serverBuilder.build();

            LOG.info("Starting gRPC server on port {}.", config.getGrpcPort());
            try {
                this.server.start();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Started gRPC server on port {}.", config.getGrpcPort());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!server.isShutdown()) {
            long timeoutMs = config.getGrpcServerShutdownTimeoutMs();
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
