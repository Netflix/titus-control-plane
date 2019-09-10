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

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.client.clustermembership.grpc.ReactorClusterMembershipClient;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.common.reactor.client.DefaultGrpcToReactorClientFactory;
import com.netflix.titus.runtime.connector.common.reactor.server.DefaultGrpcToReactorServerFactory;
import com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
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

    private ClusterMembershipGrpcServer server;
    private ManagedChannel channel;
    private ReactorClusterMembershipClient client;

    public ClusterMembershipServerResource(ClusterMembershipService service) {
        this.service = service;
    }

    @Override
    protected void before() {
        when(grpcEndpointConfiguration.getPort()).thenReturn(0);

        server = new ClusterMembershipGrpcServer(
                grpcEndpointConfiguration,
                new ReactorClusterMembershipGrpcService(
                        service,
                        titusRuntime
                ),
                new DefaultGrpcToReactorServerFactory(AnonymousCallMetadataResolver.getInstance()),
                titusRuntime
        );
        server.start();

        this.channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext(true)
                .build();

        this.client = new DefaultGrpcToReactorClientFactory(
                grpcRequestConfiguration,
                AnonymousCallMetadataResolver.getInstance()
        ).apply(
                ClusterMembershipServiceGrpc.newStub(channel),
                ReactorClusterMembershipClient.class,
                ClusterMembershipServiceGrpc.getServiceDescriptor()
        );
    }

    @Override
    protected void after() {
        if (server != null) {
            server.shutdown();
            channel.shutdownNow();
        }
    }

    public ReactorClusterMembershipClient getClient() {
        return client;
    }

    public ClusterMembershipService getService() {
        return service;
    }
}
