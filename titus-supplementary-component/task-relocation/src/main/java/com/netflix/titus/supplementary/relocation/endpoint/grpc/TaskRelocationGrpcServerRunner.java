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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import java.time.Duration;
import java.util.Collections;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipGrpcExceptionMapper;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ReactorClusterMembershipGrpcService;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.TitusGrpcServer;

@Singleton
public class TaskRelocationGrpcServerRunner {

    private final TitusGrpcServer server;

    @Inject
    public TaskRelocationGrpcServerRunner(GrpcEndpointConfiguration configuration,
                                          ReactorClusterMembershipGrpcService reactorClusterMembershipGrpcService,
                                          ReactorTaskRelocationGrpcService reactorTaskRelocationGrpcService,
                                          GrpcToReactorServerFactory reactorServerFactory,
                                          TitusRuntime titusRuntime) {
        this.server = apply(TitusGrpcServer.newBuilder(configuration.getPort(), titusRuntime))
                .withCallMetadataInterceptor()
                .withShutdownTime(Duration.ofMillis(configuration.getShutdownTimeoutMs()))

                // Cluster membership service
                .withService(
                        reactorServerFactory.apply(
                                ClusterMembershipServiceGrpc.getServiceDescriptor(),
                                reactorClusterMembershipGrpcService
                        ),
                        Collections.emptyList()
                )
                .withExceptionMapper(ClusterMembershipGrpcExceptionMapper.getInstance())

                // Relocation service
                .withService(
                        reactorServerFactory.apply(
                                TaskRelocationServiceGrpc.getServiceDescriptor(),
                                reactorTaskRelocationGrpcService
                        ),
                        Collections.emptyList()
                )
                .withExceptionMapper(ClusterMembershipGrpcExceptionMapper.getInstance())
                .build();
        server.start();
    }

    @PreDestroy
    public void shutdown() {
        server.shutdown();
    }

    public TitusGrpcServer getServer() {
        return server;
    }

    protected TitusGrpcServer.Builder apply(TitusGrpcServer.Builder serverBuilder) {
        return serverBuilder;
    }
}
