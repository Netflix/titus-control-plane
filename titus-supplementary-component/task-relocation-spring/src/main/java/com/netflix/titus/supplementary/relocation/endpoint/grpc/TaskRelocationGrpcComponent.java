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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class TaskRelocationGrpcComponent {

    @Bean
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(Environment environment) {
        return new GrpcEndpointConfigurationBean(environment, "titus.relocation.endpoint.");
    }

    @Bean
    public TaskRelocationGrpcService getTaskRelocationGrpcService(ReadOnlyJobOperations jobOperations,
                                                                  RelocationWorkflowExecutor workflowExecutor,
                                                                  TaskRelocationResultStore archiveStore) {
        return new TaskRelocationGrpcService(jobOperations, workflowExecutor, archiveStore);
    }

    @Bean
    public TaskRelocationGrpcServer getTaskRelocationGrpcServer(GrpcEndpointConfiguration configuration,
                                                                TaskRelocationGrpcService taskRelocationGrpcService) {
        return new TaskRelocationGrpcServer(configuration, taskRelocationGrpcService);
    }
}
