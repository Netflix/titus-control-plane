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

package com.netflix.titus.gateway.endpoint;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.gateway.endpoint.v3.grpc.DefaultAgentManagementServiceGrpc;
import com.netflix.titus.gateway.endpoint.v3.grpc.DefaultSchedulerServiceGrpc;
import com.netflix.titus.gateway.endpoint.v3.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.gateway.endpoint.v3.grpc.TitusGatewayGrpcServer;
import com.netflix.titus.gateway.eviction.EvictionModule;
import com.netflix.titus.gateway.eviction.GatewayGrpcEvictionService;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceImplBase;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleCallMetadataResolverProvider;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultAutoScalingServiceGrpc;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultHealthServiceGrpc;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultJobManagementServiceGrpc;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultLoadBalancerServiceGrpc;

public class GrpcModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new EvictionModule());

        bind(HealthGrpc.HealthImplBase.class).to(DefaultHealthServiceGrpc.class);
        bind(JobManagementServiceGrpc.JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
        bind(AgentManagementServiceImplBase.class).to(DefaultAgentManagementServiceGrpc.class);
        bind(AutoScalingServiceGrpc.AutoScalingServiceImplBase.class).to(DefaultAutoScalingServiceGrpc.class);
        bind(LoadBalancerServiceGrpc.LoadBalancerServiceImplBase.class).to(DefaultLoadBalancerServiceGrpc.class);
        bind(SchedulerServiceGrpc.SchedulerServiceImplBase.class).to(DefaultSchedulerServiceGrpc.class);
        bind(TitusGatewayGrpcServer.class).asEagerSingleton();
        bind(CallMetadataResolver.class).toProvider(SimpleCallMetadataResolverProvider.class);
    }

    @Provides
    @Singleton
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcEndpointConfiguration.class);
    }
}
