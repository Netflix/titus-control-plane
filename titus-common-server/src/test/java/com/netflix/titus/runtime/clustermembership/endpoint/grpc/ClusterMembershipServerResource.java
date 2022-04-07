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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import java.time.Duration;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.client.clustermembership.grpc.ClusterMembershipClient;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.runtime.clustermembership.DefaultClusterMembershipClient;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.TitusGrpcServer;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.DefaultGrpcClientCallAssistantFactory;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.DefaultGrpcServerCallAssistant;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.GrpcCallAssistantConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.rules.ExternalResource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterMembershipServerResource extends ExternalResource {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final CommonGrpcEndpointConfiguration grpcEndpointConfiguration = mock(CommonGrpcEndpointConfiguration.class);
    private final GrpcRequestConfiguration grpcRequestConfiguration = Archaius2Ext.newConfiguration(GrpcRequestConfiguration.class);

    private final ClusterMembershipService service;

    private TitusGrpcServer server;
    private ManagedChannel channel;
    private ClusterMembershipClient client;

    public ClusterMembershipServerResource(ClusterMembershipService service) {
        this.service = service;
    }

    @Override
    protected void before() {
        when(grpcEndpointConfiguration.getPort()).thenReturn(0);

        DefaultGrpcServerCallAssistant grpcServerCallAssistant = new DefaultGrpcServerCallAssistant(AnonymousCallMetadataResolver.getInstance(), NoOpHostCallerIdResolver.getInstance());
        server = TitusGrpcServer.newBuilder(0, titusRuntime)
                .withServerConfigurer(builder ->
                        builder.addService(new GrpcClusterMembershipService(service, grpcServerCallAssistant, titusRuntime))
                )
                .withExceptionMapper(ClusterMembershipGrpcExceptionMapper.getInstance())
                .withShutdownTime(Duration.ZERO)
                .build();
        server.start();

        this.channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        GrpcCallAssistantConfiguration configuration = Archaius2Ext.newConfiguration(GrpcCallAssistantConfiguration.class);
        DefaultGrpcClientCallAssistantFactory grpcClientCallAssistantFactory = new DefaultGrpcClientCallAssistantFactory(
                configuration, AnonymousCallMetadataResolver.getInstance()
        );
        this.client = new DefaultClusterMembershipClient(grpcClientCallAssistantFactory, channel);
    }

    @Override
    protected void after() {
        if (server != null) {
            server.shutdown();
            channel.shutdownNow();
        }
    }

    public ClusterMembershipClient getClient() {
        return client;
    }

    public ClusterMembershipService getService() {
        return service;
    }
}
