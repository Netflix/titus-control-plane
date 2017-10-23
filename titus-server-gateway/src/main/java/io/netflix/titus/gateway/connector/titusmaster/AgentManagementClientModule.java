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

package io.netflix.titus.gateway.connector.titusmaster;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.gateway.startup.TitusGatewayConfiguration;

public class AgentManagementClientModule extends AbstractModule {

    public static final String NAME = "agentManagementService";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(NAME)
    Channel managedChannel(TitusGatewayConfiguration configuration, LeaderResolver leaderResolver, TitusRuntime titusRuntime) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort(), titusRuntime))
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Provides
    @Singleton
    AgentManagementServiceStub managementClient(final @Named(NAME) Channel channel) {
        return AgentManagementServiceGrpc.newStub(channel);
    }
}
