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

import com.netflix.titus.runtime.connector.MasterConnectorComponent;
import com.netflix.titus.runtime.connector.MasterDataReplicationComponent;
import com.netflix.titus.runtime.connector.titusmaster.ConfigurationLeaderResolverComponent;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterConnectorComponent;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.endpoint.rest.RestAddOnsComponent;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcServer;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcService;
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
        RestAddOnsComponent.class
})
public class RelocationMain {

    @Bean
    public TaskRelocationGrpcServer getTaskRelocationGrpcServer(GrpcEndpointConfiguration configuration,
                                                                TaskRelocationGrpcService taskRelocationGrpcService) {
        return new TaskRelocationGrpcServer(configuration, taskRelocationGrpcService);
    }

    public static void main(String[] args) {
        SpringApplication.run(RelocationMain.class, args);
    }
}
