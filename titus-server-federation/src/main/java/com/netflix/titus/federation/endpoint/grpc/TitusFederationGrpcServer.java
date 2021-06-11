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

package com.netflix.titus.federation.endpoint.grpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.common.util.loadshedding.grpc.GrpcAdmissionControllerServerInterceptor;
import com.netflix.titus.federation.endpoint.EndpointConfiguration;
import com.netflix.titus.grpc.protogen.*;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import com.netflix.titus.grpc.protogen.HealthGrpc.HealthImplBase;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceImplBase;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc.JobActivityHistoryServiceImplBase;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceImplBase;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.runtime.machine.ReactorGatewayMachineGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class TitusFederationGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(TitusFederationGrpcServer.class);

    private final GrpcAdmissionControllerServerInterceptor admissionController;
    private final HealthImplBase healthService;
    private final SchedulerServiceImplBase schedulerService;
    private final JobManagementServiceImplBase jobManagementService;
    private AutoScalingServiceImplBase autoScalingService;
    private LoadBalancerServiceImplBase loadBalancerService;
    private JobActivityHistoryServiceImplBase jobActivityHistoryService;
    private final ReactorGatewayMachineGrpcService reactorMachineGrpcService;
    private final GrpcToReactorServerFactory reactorServerFactory;
    private final EndpointConfiguration config;
    private final TitusRuntime runtime;
    private final ExecutorService grpcCallbackExecutor;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;
    private int port;

    @Inject
    public TitusFederationGrpcServer(
            GrpcAdmissionControllerServerInterceptor admissionController,
            HealthImplBase healthService,
            SchedulerServiceImplBase schedulerService,
            JobManagementServiceImplBase jobManagementService,
            AutoScalingServiceImplBase autoScalingService,
            LoadBalancerServiceImplBase loadBalancerService,
            JobActivityHistoryServiceImplBase jobActivityHistoryService,
            ReactorGatewayMachineGrpcService reactorMachineGrpcService,
            GrpcToReactorServerFactory reactorServerFactory,
            EndpointConfiguration config,
            TitusRuntime runtime) {
        this.admissionController = admissionController;
        this.healthService = healthService;
        this.schedulerService = schedulerService;
        this.jobManagementService = jobManagementService;
        this.autoScalingService = autoScalingService;
        this.loadBalancerService = loadBalancerService;
        this.jobActivityHistoryService = jobActivityHistoryService;
        this.reactorMachineGrpcService = reactorMachineGrpcService;
        this.reactorServerFactory = reactorServerFactory;
        this.config = config;
        this.runtime = runtime;
        this.grpcCallbackExecutor = ExecutorsExt.instrumentedCachedThreadPool(runtime.getRegistry(), "grpcCallbackExecutor");
    }

    public int getGrpcPort() {
        return port;
    }

    @PostConstruct
    public void start() {
        if (started.getAndSet(true)) {
            return;
        }
        this.port = config.getGrpcPort();
        this.server = configure(ServerBuilder.forPort(port).executor(grpcCallbackExecutor))
                .addService(ServerInterceptors.intercept(
                        healthService,
                        createInterceptors(HealthGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        schedulerService,
                        createInterceptors(SchedulerServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        jobManagementService,
                        createInterceptors(JobManagementServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        autoScalingService,
                        createInterceptors(AutoScalingServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        loadBalancerService,
                        createInterceptors(LoadBalancerServiceGrpc.getServiceDescriptor())))
                .addService(ServerInterceptors.intercept(
                        jobActivityHistoryService,
                        createInterceptors(JobActivityHistoryServiceGrpc.getServiceDescriptor())))
                .addService(
                        ServerInterceptors.intercept(
                                reactorServerFactory.apply(
                                        MachineServiceGrpc.getServiceDescriptor(),
                                        reactorMachineGrpcService
                                ),
                                createInterceptors(MachineServiceGrpc.getServiceDescriptor())
                        )
                )
                .addService(ProtoReflectionService.newInstance())
                .build();

        LOG.info("Starting gRPC server on port {}.", port);
        try {
            this.server.start();
            this.port = server.getPort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Started gRPC server on port {}.", port);
    }

    @PreDestroy
    public void shutdown() {
        if (server.isShutdown()) {
            return;
        }
        long timeoutMs = config.getGrpcServerShutdownTimeoutMs();
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
        return Arrays.asList(
                admissionController,
                new ErrorCatchingServerInterceptor(),
                new V3HeaderInterceptor()
        );
    }
}
