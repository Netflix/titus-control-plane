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

package com.netflix.titus.runtime.connector.titusmaster;

import java.util.Collections;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.connector.cloud.noop.NoOpLoadBalancerConnector;
import com.netflix.titus.api.loadbalancer.model.sanitizer.DefaultLoadBalancerResourceValidator;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.network.http.HttpClient;
import com.netflix.titus.common.network.http.RxHttpClient;
import com.netflix.titus.common.network.http.internal.okhttp.CompositeRetryInterceptor;
import com.netflix.titus.common.network.http.internal.okhttp.EndpointResolverInterceptor;
import com.netflix.titus.common.network.http.internal.okhttp.OkHttpClient;
import com.netflix.titus.common.network.http.internal.okhttp.PassthroughInterceptor;
import com.netflix.titus.common.network.http.internal.okhttp.RxOkHttpClient;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.grpc.protogen.*;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceStub;
import com.netflix.titus.grpc.protogen.HealthGrpc.HealthStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceStub;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceStub;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.common.reactor.DefaultGrpcToReactorClientFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CommonCallMetadataUtils;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import okhttp3.Interceptor;

public class TitusMasterConnectorModule extends AbstractModule {
    public static final String MANAGED_CHANNEL_NAME = "ManagedChannel";
    public static final String TITUS_MASTER_CLIENT = "TitusMaster";
    public static final String RX_TITUS_MASTER_CLIENT = "RxTitusMaster";

    private static final int NUMBER_OF_RETRIES = 3;
    private static final int DEFAULT_CONNECT_TIMEOUT = 10_000;
    private static final int DEFAULT_READ_TIMEOUT = 30_000;

    @Override
    protected void configure() {
        bind(LeaderResolver.class).to(ConfigurationLeaderResolver.class);
        bind(LoadBalancerResourceValidator.class).to(DefaultLoadBalancerResourceValidator.class);
        bind(LoadBalancerConnector.class).to(NoOpLoadBalancerConnector.class);
    }

    @Provides
    @Singleton
    public TitusMasterClientConfiguration getTitusMasterClientConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusMasterClientConfiguration.class);
    }

    @Named(TITUS_MASTER_CLIENT)
    @Provides
    @Singleton
    public HttpClient httpClient() {
        OkHttpClient.Builder builder = OkHttpClient.newBuilder();
        Interceptor retryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(new PassthroughInterceptor()), NUMBER_OF_RETRIES);
        builder.interceptor(retryInterceptor)
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .readTimeout(DEFAULT_READ_TIMEOUT);
        return builder.build();
    }

    @Named(RX_TITUS_MASTER_CLIENT)
    @Provides
    @Singleton
    public RxHttpClient rxHttpClient(LeaderResolver leaderResolver) {
        RxOkHttpClient.Builder builder = RxOkHttpClient.newBuilder();
        LeaderEndpointResolver endpointResolver = new LeaderEndpointResolver(leaderResolver);
        Interceptor retryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(new EndpointResolverInterceptor(endpointResolver)), NUMBER_OF_RETRIES);
        builder.interceptor(retryInterceptor)
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .readTimeout(DEFAULT_READ_TIMEOUT);
        return builder.build();
    }

    @Provides
    @Singleton
    @Named(MANAGED_CHANNEL_NAME)
    Channel managedChannel(TitusMasterClientConfiguration configuration, LeaderResolver leaderResolver, TitusRuntime titusRuntime) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort(), titusRuntime))
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Provides
    @Singleton
    public GrpcToReactorClientFactory getReactorGrpcClientAdapterFactory(GrpcRequestConfiguration configuration,
                                                                         CallMetadataResolver callMetadataResolver) {
        return new DefaultGrpcToReactorClientFactory<>(configuration,
                CommonCallMetadataUtils.newGrpcStubDecorator(callMetadataResolver),
                CallMetadata.class
        );
    }

    @Provides
    @Singleton
    HealthStub healthClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return HealthGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    SupervisorServiceGrpc.SupervisorServiceStub supervisorClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return SupervisorServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    AgentManagementServiceStub managementClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return AgentManagementServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    JobManagementServiceStub jobManagementClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return JobManagementServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    SchedulerServiceStub schedulerClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return SchedulerServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    LoadBalancerServiceStub loadBalancerClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return LoadBalancerServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    AutoScalingServiceStub autoScalingClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return AutoScalingServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub jobActivityHistoryClient(final @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return JobActivityHistoryServiceGrpc.newStub(channel);
    }
}
