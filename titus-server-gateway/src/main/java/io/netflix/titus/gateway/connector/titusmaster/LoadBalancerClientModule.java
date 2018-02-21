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
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.connector.cloud.noop.NoOpLoadBalancerConnector;
import io.netflix.titus.api.loadbalancer.model.sanitizer.DefaultLoadBalancerResourceValidator;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.gateway.startup.TitusGatewayConfiguration;

public class LoadBalancerClientModule extends AbstractModule {
    public static final String NAME = "titusLoadBalancerService";

    @Override
    protected void configure() {
        bind(LoadBalancerResourceValidator.class).to(DefaultLoadBalancerResourceValidator.class);
        bind(LoadBalancerConnector.class).to(NoOpLoadBalancerConnector.class);
    }

    @Provides
    @Singleton
    @Named(NAME)
    Channel managedChannel(TitusGatewayConfiguration configuration, LeaderResolver leaderResolver, TitusRuntime titusRuntime) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort(), titusRuntime))
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Provides
    @Singleton
    LoadBalancerServiceGrpc.LoadBalancerServiceStub titusLoadBalancerClient(final @Named(NAME) Channel channel) {
        return LoadBalancerServiceGrpc.newStub(channel);
    }
}
