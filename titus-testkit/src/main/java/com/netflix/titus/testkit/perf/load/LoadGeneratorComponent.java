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

package com.netflix.titus.testkit.perf.load;

import javax.inject.Named;

import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.runtime.connector.agent.AgentManagementDataReplicationComponent;
import com.netflix.titus.runtime.connector.agent.AgentManagerConnectorComponent;
import com.netflix.titus.runtime.connector.agent.ReactorAgentManagementServiceStub;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorClientFactoryComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionConnectorComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementDataReplicationComponent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagerConnectorComponent;
import com.netflix.titus.testkit.embedded.cloud.connector.remote.SimulatedRemoteInstanceCloudConnector;
import com.netflix.titus.testkit.perf.load.runner.AgentTerminator;
import com.netflix.titus.testkit.perf.load.runner.JobTerminator;
import com.netflix.titus.testkit.perf.load.runner.Orchestrator;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        GrpcToReactorClientFactoryComponent.class,

        // Agent connector
        AgentManagerConnectorComponent.class,
        AgentManagementDataReplicationComponent.class,

        // Job connector
        JobManagerConnectorComponent.class,
        JobManagementDataReplicationComponent.class,

        // Eviction connector
        EvictionConnectorComponent.class,
})
public class LoadGeneratorComponent {

    @Bean
    public ExecutionContext getExecutionContext(JobManagementClient jobManagementClient,
                                                ReadOnlyJobOperations cachedJobManagementClient,
                                                ReactorAgentManagementServiceStub agentManagementClient,
                                                ReadOnlyAgentOperations cachedAgentManagementClient,
                                                EvictionServiceClient evictionServiceClient,
                                                @Named(SimulatedRemoteInstanceCloudConnector.SIMULATED_CLOUD) Channel cloudSimulatorGrpcChannel) {
        return new ExecutionContext(
                jobManagementClient,
                cachedJobManagementClient,
                agentManagementClient,
                cachedAgentManagementClient,
                evictionServiceClient,
                cloudSimulatorGrpcChannel
        );
    }

    @Bean
    public Orchestrator getOrchestrator(ExecutionContext context) {
        return new Orchestrator(context);
    }

    @Bean
    public AgentTerminator getAgentTerminator(ExecutionContext context) {
        return new AgentTerminator(context);
    }

    @Bean
    public JobTerminator getJobTerminator(ExecutionContext context) {
        return new JobTerminator(context);
    }
}
