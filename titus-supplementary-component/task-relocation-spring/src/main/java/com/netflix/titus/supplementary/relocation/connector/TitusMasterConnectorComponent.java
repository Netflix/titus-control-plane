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

package com.netflix.titus.supplementary.relocation.connector;

import javax.inject.Named;

import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.connector.cloud.noop.NoOpLoadBalancerConnector;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolver;
import com.netflix.titus.runtime.connector.titusmaster.LeaderResolver;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterClientConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.DefaultReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import rx.Completable;

@Configuration
public class TitusMasterConnectorComponent {

    public static final String TITUS_MASTER_CHANNEL = "TitusMasterChannel";

    @Bean
    public TitusMasterClientConfiguration getTitusMasterClientConfiguration(Environment environment) {
        return new TitusMasterClientConfigurationBean(environment);
    }

    @Bean
    public LeaderResolver getLeaderResolver(TitusMasterClientConfiguration configuration) {
        return new ConfigurationLeaderResolver(configuration);
    }

    /**
     * TODO Implement
     */
    @Bean
    public LoadBalancerResourceValidator getLoadBalancerResourceValidator() {
        return loadBalancerId -> Completable.error(new IllegalStateException("not implemented"));
    }

    @Bean
    public LoadBalancerConnector getLoadBalancerConnector() {
        return new NoOpLoadBalancerConnector();
    }

    @Bean
    public ReactorGrpcClientAdapterFactory getReactorGrpcClientAdapterFactory(GrpcClientConfiguration configuration,
                                                                              CallMetadataResolver callMetadataResolver) {
        return new DefaultReactorGrpcClientAdapterFactory(configuration, callMetadataResolver);
    }

    @Bean
    public HealthGrpc.HealthStub healthClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return HealthGrpc.newStub(channel);
    }

    @Bean
    public SupervisorServiceGrpc.SupervisorServiceStub supervisorClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return SupervisorServiceGrpc.newStub(channel);
    }

    @Bean
    public AgentManagementServiceGrpc.AgentManagementServiceStub managementClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return AgentManagementServiceGrpc.newStub(channel);
    }

    @Bean
    public JobManagementServiceGrpc.JobManagementServiceStub jobManagementClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return JobManagementServiceGrpc.newStub(channel);
    }

    @Bean
    public SchedulerServiceGrpc.SchedulerServiceStub schedulerClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return SchedulerServiceGrpc.newStub(channel);
    }

    @Bean
    public EvictionServiceGrpc.EvictionServiceStub evictionClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return EvictionServiceGrpc.newStub(channel);
    }

    @Bean
    public LoadBalancerServiceGrpc.LoadBalancerServiceStub loadBalancerClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return LoadBalancerServiceGrpc.newStub(channel);
    }

    @Bean
    public AutoScalingServiceGrpc.AutoScalingServiceStub autoScalingClient(final @Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return AutoScalingServiceGrpc.newStub(channel);
    }
}
