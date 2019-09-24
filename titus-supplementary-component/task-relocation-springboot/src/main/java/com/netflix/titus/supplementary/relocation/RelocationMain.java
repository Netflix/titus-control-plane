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

package com.netflix.titus.supplementary.relocation;

import javax.inject.Named;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthIndicators;
import com.netflix.titus.common.jhiccup.JHiccupComponent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.runtime.clustermembership.connector.ClusterMembershipInMemoryConnectorComponent;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipGrpcEndpointComponent;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ReactorClusterMembershipGrpcService;
import com.netflix.titus.runtime.clustermembership.endpoint.rest.ClusterMembershipRestEndpointComponent;
import com.netflix.titus.runtime.clustermembership.service.ClusterMembershipServiceComponent;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationComponent;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationConfiguration;
import com.netflix.titus.runtime.connector.agent.AgentManagementDataReplicationComponent;
import com.netflix.titus.runtime.connector.agent.AgentManagerConnectorComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorClientFactoryComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorServerFactoryComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionConnectorComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicationComponent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementDataReplicationComponent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagerConnectorComponent;
import com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.endpoint.rest.RestAddOnsComponent;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.ReactorTaskRelocationGrpcService;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcServerRunner;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import com.netflix.titus.supplementary.relocation.workflow.DefaultRelocationWorkflowExecutor;
import io.grpc.Channel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import static com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent.TITUS_MASTER_CHANNEL;

@SpringBootApplication
@Import({
        RelocationTitusRuntimeComponent.class,
        JHiccupComponent.class,
        CallMetadataResolveComponent.class,
        ConfigurationLeaderResolverComponent.class,
        GrpcToReactorClientFactoryComponent.class,
        GrpcToReactorServerFactoryComponent.class,

        // Cluster membership service
        ClusterMembershipInMemoryConnectorComponent.class,
        ClusterMembershipServiceComponent.class,
        ClusterMembershipGrpcEndpointComponent.class,
        ClusterMembershipRestEndpointComponent.class,
        LeaderActivationComponent.class,

        // Agent connector
        AgentManagerConnectorComponent.class,
        AgentManagementDataReplicationComponent.class,

        // Job connector
        JobManagerConnectorComponent.class,
        JobManagementDataReplicationComponent.class,

        // Eviction connector
        EvictionConnectorComponent.class,
        EvictionDataReplicationComponent.class,

        RestAddOnsComponent.class
})
public class RelocationMain {

    @Bean
    public HealthIndicator getHealthIndicator() {
        return HealthIndicators.alwaysHealthy();
    }

    @Bean
    @Named(AgentManagerConnectorComponent.AGENT_CHANNEL)
    public Channel getAgentManagerChannel(@Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    @Named(JobManagerConnectorComponent.JOB_MANAGER_CHANNEL)
    public Channel getJobManagerChannel(@Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    @Named(EvictionConnectorComponent.EVICTION_CHANNEL)
    public Channel getEvictionChannel(@Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    public RelocationLeaderActivator getRelocationLeaderActivator(LeaderActivationConfiguration configuration,
                                                                  TaskRelocationStore relocationStore,
                                                                  DefaultRelocationWorkflowExecutor workflowExecutor,
                                                                  ClusterMembershipService membershipService,
                                                                  TitusRuntime titusRuntime) {
        return new RelocationLeaderActivator(configuration, relocationStore, workflowExecutor, membershipService, titusRuntime);
    }

    @Bean
    public TaskRelocationGrpcServerRunner getTaskRelocationGrpcServerRunner(GrpcEndpointConfiguration configuration,
                                                                            ReactorClusterMembershipGrpcService reactorClusterMembershipGrpcService,
                                                                            ReactorTaskRelocationGrpcService reactorTaskRelocationGrpcService,
                                                                            GrpcToReactorServerFactory reactorServerFactory,
                                                                            TitusRuntime titusRuntime) {
        return new TaskRelocationGrpcServerRunner(configuration, reactorClusterMembershipGrpcService, reactorTaskRelocationGrpcService, reactorServerFactory, titusRuntime);
    }

    public static void main(String[] args) {
        SpringApplication.run(RelocationMain.class, args);
    }
}
