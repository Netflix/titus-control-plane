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

package io.netflix.titus.gateway.endpoint;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.gateway.endpoint.v3.grpc.DefaultAgentManagementServiceGrpc;
import io.netflix.titus.gateway.endpoint.v3.grpc.DefaultAutoScalingServiceGrpc;
import io.netflix.titus.gateway.endpoint.v3.grpc.DefaultJobManagementServiceGrpc;
import io.netflix.titus.gateway.endpoint.v3.grpc.DefaultLoadBalancerServiceGrpc;
import io.netflix.titus.gateway.endpoint.v3.grpc.DefaultSchedulerServiceGrpc;
import io.netflix.titus.gateway.endpoint.v3.grpc.GrpcEndpointConfiguration;
import io.netflix.titus.gateway.endpoint.v3.grpc.TitusGatewayGrpcServer;

public class GrpcModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(JobManagementServiceGrpc.JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
        bind(AgentManagementServiceImplBase.class).to(DefaultAgentManagementServiceGrpc.class);
        bind(AutoScalingServiceGrpc.AutoScalingServiceImplBase.class).to(DefaultAutoScalingServiceGrpc.class);
        bind(LoadBalancerServiceGrpc.LoadBalancerServiceImplBase.class).to(DefaultLoadBalancerServiceGrpc.class);
        bind(SchedulerServiceGrpc.SchedulerServiceImplBase.class).to(DefaultSchedulerServiceGrpc.class);
        bind(TitusGatewayGrpcServer.class).asEagerSingleton();
        bind(SessionContext.class).to(AnonymousSessionContext.class);
    }

    @Provides
    @Singleton
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcEndpointConfiguration.class);
    }
}
