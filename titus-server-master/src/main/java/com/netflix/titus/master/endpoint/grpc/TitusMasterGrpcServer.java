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

package com.netflix.titus.master.endpoint.grpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.framework.fit.adapter.GrpcFitInterceptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.common.util.loadshedding.grpc.GrpcAdmissionControllerServerInterceptor;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceImplBase;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc.HealthImplBase;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceImplBase;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceImplBase;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceImplBase;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc;
import com.netflix.titus.master.endpoint.common.grpc.interceptor.LeaderServerInterceptor;
import com.netflix.titus.master.machine.endpoint.grpc.ReactorMasterMachineGrpcService;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Singleton
public class TitusMasterGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(TitusMasterGrpcServer.class);

    private final HealthImplBase healthService;
    private final SupervisorServiceImplBase titusSupervisorService;
    private final JobManagementServiceImplBase jobManagementService;
    private final AgentManagementServiceImplBase agentManagementService;
    private final EvictionServiceImplBase evictionService;
    private final AutoScalingServiceImplBase appAutoScalingService;
    private final SchedulerServiceImplBase schedulerService;
    private final GrpcMasterEndpointConfiguration config;
    private final LeaderServerInterceptor leaderServerInterceptor;
    private final LoadBalancerServiceImplBase loadBalancerService;
    private final GrpcAdmissionControllerServerInterceptor admissionControllerServerInterceptor;
    private final ReactorMasterMachineGrpcService reactorMachineGrpcService;
    private final GrpcToReactorServerFactory reactorServerFactory;
    private final TitusRuntime titusRuntime;
    private final ExecutorService grpcCallbackExecutor;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;
    private int port;

    @Inject
    public TitusMasterGrpcServer(
            HealthImplBase healthService,
            SupervisorServiceImplBase titusSupervisorService,
            JobManagementServiceImplBase jobManagementService,
            AgentManagementServiceImplBase agentManagementService,
            EvictionServiceImplBase evictionService,
            AutoScalingServiceImplBase appAutoScalingService,
            LoadBalancerServiceImplBase loadBalancerService,
            SchedulerServiceImplBase schedulerService,
            GrpcMasterEndpointConfiguration config,
            LeaderServerInterceptor leaderServerInterceptor,
            GrpcAdmissionControllerServerInterceptor admissionControllerServerInterceptor,
            ReactorMasterMachineGrpcService reactorMachineGrpcService,
            GrpcToReactorServerFactory reactorServerFactory,
            TitusRuntime titusRuntime
    ) {
        this.healthService = healthService;
        this.titusSupervisorService = titusSupervisorService;
        this.jobManagementService = jobManagementService;
        this.agentManagementService = agentManagementService;
        this.evictionService = evictionService;
        this.appAutoScalingService = appAutoScalingService;
        this.loadBalancerService = loadBalancerService;
        this.schedulerService = schedulerService;
        this.config = config;
        this.leaderServerInterceptor = leaderServerInterceptor;
        this.admissionControllerServerInterceptor = admissionControllerServerInterceptor;
        this.reactorMachineGrpcService = reactorMachineGrpcService;
        this.reactorServerFactory = reactorServerFactory;
        this.titusRuntime = titusRuntime;
        this.grpcCallbackExecutor = ExecutorsExt.instrumentedCachedThreadPool(titusRuntime.getRegistry(), "grpcCallbackExecutor");
    }

    public int getGrpcPort() {
        return port;
    }

    @PostConstruct
    public void start() {
        if (started.getAndSet(true)) {
            return;
        }
        this.port = config.getPort();
        this.server = configure(ServerBuilder.forPort(port).executor(grpcCallbackExecutor))
                .addService(ServerInterceptors.intercept(
                        healthService,
                        createInterceptors(HealthGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        titusSupervisorService,
                        createInterceptors(SupervisorServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        jobManagementService,
                        createInterceptors(JobManagementServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        agentManagementService,
                        createInterceptors(AgentManagementServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        evictionService,
                        createInterceptors(EvictionServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        appAutoScalingService,
                        createInterceptors(AutoScalingServiceGrpc.getServiceDescriptor())
                ))
                .addService(ServerInterceptors.intercept(
                        schedulerService,
                        createInterceptors(SchedulerServiceGrpc.getServiceDescriptor())
                ))
                .addService(
                        ServerInterceptors.intercept(
                                reactorServerFactory.apply(
                                        MachineServiceGrpc.getServiceDescriptor(),
                                        reactorMachineGrpcService
                                ),
                                createInterceptors(MachineServiceGrpc.getServiceDescriptor())
                        )
                )
                .addService(ServerInterceptors.intercept(
                        loadBalancerService,
                        createInterceptors(LoadBalancerServiceGrpc.getServiceDescriptor())
                ))
                .build();

        LOG.info("Starting gRPC server on port {}.", port);
        try {
            this.server.start();
            this.port = server.getPort();
        } catch (final IOException e) {
            String errorMessage = "Cannot start TitusMaster GRPC server on port " + port;
            LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
        LOG.info("Started gRPC server on port {}.", port);
    }

    @PreDestroy
    public void shutdown() {
        if (server.isShutdown()) {
            return;
        }
        long timeoutMs = config.getShutdownTimeoutMs();
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
        List<ServerInterceptor> interceptors = new ArrayList<ServerInterceptor>();
        interceptors.add(new ErrorCatchingServerInterceptor());
        interceptors.add(leaderServerInterceptor);
        interceptors.add(new V3HeaderInterceptor());
        interceptors.add(admissionControllerServerInterceptor);
        return GrpcFitInterceptor.appendIfFitEnabled(interceptors, titusRuntime);
    }
}
