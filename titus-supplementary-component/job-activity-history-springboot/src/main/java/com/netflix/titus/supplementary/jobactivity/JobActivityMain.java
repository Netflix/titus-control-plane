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

import com.netflix.titus.runtime.connector.MasterConnectorComponent;
import com.netflix.titus.runtime.connector.MasterDataReplicationComponent;
import com.netflix.titus.runtime.connector.agent.AgentManagementDataReplicationComponent;
import com.netflix.titus.runtime.connector.agent.AgentManagerConnectorComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionConnectorComponent;
import com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterConnectorComponent;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterConnectorModule;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.endpoint.rest.RestAddOnsComponent;
import com.netflix.titus.supplementary.jobactivity.endpoint.grpc.JobActivityGrpcServer;
import com.netflix.titus.supplementary.jobactivity.endpoint.grpc.JobActivityGrpcService;
import io.grpc.Channel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({
        CallMetadataResolveComponent.class,
        ConfigurationLeaderResolverComponent.class,
        TitusMasterConnectorComponent.class,
        MasterConnectorComponent.class,
        MasterDataReplicationComponent.class,
        AgentManagerConnectorComponent.class,
        AgentManagementDataReplicationComponent.class,
        EvictionConnectorComponent.class,
        RestAddOnsComponent.class
})
public class JobActivityMain {

    @Bean
    public JobActivityGrpcServer getJobActivityHistoryGrpcServer(GrpcEndpointConfiguration configuration,
                                                                 JobActivityGrpcService jobActivityGrpcService) {
        return new JobActivityGrpcServer(configuration, jobActivityGrpcService);
    }

    @Bean(name = TitusMasterConnectorModule.MANAGED_CHANNEL_NAME)
    public Channel getChannel(@Qualifier(TitusMasterConnectorComponent.TITUS_MASTER_CHANNEL) Channel channel) {
        return channel;
    }

    public static void main(String[] args) {
        SpringApplication.run(JobActivityMain.class, args);
    }
}
