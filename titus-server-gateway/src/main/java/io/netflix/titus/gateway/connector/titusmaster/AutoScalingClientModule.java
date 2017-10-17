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


import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.netflix.titus.gateway.startup.TitusGatewayConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;


public class AutoScalingClientModule extends AbstractModule {
    private static Logger log = LoggerFactory.getLogger(AutoScalingClientModule.class);
    public static final String NAME = "titusAutoScalingService";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(NAME)
    Channel managedChannel(TitusGatewayConfiguration configuration, LeaderResolver leaderResolver) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort()))
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();

    }


    @Provides
    @Singleton
    AutoScalingServiceGrpc.AutoScalingServiceStub titusAutoScalingClient(final @Named(NAME) Channel channel) {
        return AutoScalingServiceGrpc.newStub(channel);
    }
}
