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

package com.netflix.titus.supplementary.jobactivity;

import javax.inject.Named;

import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthIndicators;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationComponent;
import com.netflix.titus.runtime.clustermembership.connector.ClusterMembershipInMemoryConnectorComponent;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipGrpcEndpointComponent;
import com.netflix.titus.runtime.clustermembership.endpoint.rest.ClusterMembershipRestEndpointComponent;
import com.netflix.titus.runtime.clustermembership.service.ClusterMembershipServiceComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorClientFactoryComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorServerFactoryComponent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagerConnectorComponent;
import com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.endpoint.rest.RestAddOnsComponent;
import com.netflix.titus.supplementary.jobactivity.endpoint.grpc.JobActivityGrpcServer;
import com.netflix.titus.supplementary.jobactivity.endpoint.grpc.JobActivityGrpcService;
import com.netflix.titus.supplementary.jobactivity.store.JooqJobActivityContextComponent;
import io.grpc.Channel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import static com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent.TITUS_MASTER_CHANNEL;

@SpringBootApplication
@Import({
        JobActivityTitusRuntimeComponent.class,
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

        // Job connector
        JobManagerConnectorComponent.class,

        // Store
        JooqJobActivityContextComponent.class,

        RestAddOnsComponent.class
})
public class JobActivityMain {

    @Bean
    public HealthIndicator getHealthIndicator() {
        return HealthIndicators.alwaysHealthy();
    }


    @Bean
    @Named(JobManagerConnectorComponent.JOB_MANAGER_CHANNEL)
    public Channel getJobManagerChannel(@Named(TITUS_MASTER_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    public JobActivityGrpcServer getJobActivityHistoryGrpcServer(GrpcEndpointConfiguration configuration,
                                                                 JobActivityGrpcService jobActivityGrpcService,
                                                                 TitusRuntime runtime) {
        return new JobActivityGrpcServer(configuration, jobActivityGrpcService, runtime);
    }

    public static void main(String[] args) {
        SpringApplication.run(JobActivityMain.class, args);
    }
}
