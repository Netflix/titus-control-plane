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
import java.util.Arrays;
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
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceImplBase;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceImplBase;
import com.netflix.titus.master.endpoint.common.grpc.interceptor.LeaderServerInterceptor;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Singleton
public class TitusMasterGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(TitusMasterGrpcServer.class);

    private final JobManagementServiceImplBase jobManagementService;
    private final AgentManagementServiceImplBase agentManagementService;
    private AutoScalingServiceImplBase appAutoScalingService;
    private final SchedulerServiceImplBase schedulerService;
    private final GrpcEndpointConfiguration config;
    private final LeaderServerInterceptor leaderServerInterceptor;
    private final LoadBalancerServiceImplBase loadBalancerService;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public TitusMasterGrpcServer(
            JobManagementServiceImplBase jobManagementService,
            AgentManagementServiceImplBase agentManagementService,
            AutoScalingServiceImplBase appAutoScalingService,
            LoadBalancerServiceImplBase loadBalancerService,
            SchedulerServiceImplBase schedulerService,
            GrpcEndpointConfiguration config,
            LeaderServerInterceptor leaderServerInterceptor) {
        this.jobManagementService = jobManagementService;
        this.agentManagementService = agentManagementService;
        this.appAutoScalingService = appAutoScalingService;
        this.loadBalancerService = loadBalancerService;
        this.schedulerService = schedulerService;
        this.config = config;
        this.leaderServerInterceptor = leaderServerInterceptor;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = configure(ServerBuilder.forPort(config.getPort()));
            serverBuilder.addService(ServerInterceptors.intercept(
                    jobManagementService,
                    createInterceptors(JobManagementServiceGrpc.getServiceDescriptor())
            )).addService(ServerInterceptors.intercept(
                    agentManagementService,
                    createInterceptors(AgentManagementServiceGrpc.getServiceDescriptor())
            )).addService(ServerInterceptors.intercept(
                    appAutoScalingService,
                    createInterceptors(AutoScalingServiceGrpc.getServiceDescriptor())
            )).addService(ServerInterceptors.intercept(
                    schedulerService,
                    createInterceptors(SchedulerServiceGrpc.getServiceDescriptor())
            ));
            if (config.getLoadBalancerGrpcEnabled()) {
                serverBuilder.addService(ServerInterceptors.intercept(
                        loadBalancerService,
                        createInterceptors(LoadBalancerServiceGrpc.getServiceDescriptor())
                ));
            }
            this.server = serverBuilder.build();

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
        return Arrays.asList(new ErrorCatchingServerInterceptor(), leaderServerInterceptor);
    }
}
