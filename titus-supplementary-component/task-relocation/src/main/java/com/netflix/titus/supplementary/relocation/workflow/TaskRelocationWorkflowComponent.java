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

package com.netflix.titus.supplementary.relocation.workflow;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.runtime.connector.eviction.EvictionConfiguration;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.supplementary.relocation.descheduler.DeschedulerService;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TaskRelocationWorkflowComponent {
    @Bean
    public RelocationConfiguration getRelocationConfiguration(TitusRuntime titusRuntime) {
        return Archaius2Ext.newConfiguration(RelocationConfiguration.class, titusRuntime.getMyEnvironment());
    }

    @Bean
    public EvictionConfiguration getEvictionConfiguration(TitusRuntime titusRuntime) {
        return Archaius2Ext.newConfiguration(EvictionConfiguration.class, titusRuntime.getMyEnvironment());
    }

    @Bean
    public RelocationWorkflowExecutor getRelocationWorkflowExecutor(RelocationConfiguration configuration,
                                                                    NodeDataResolver nodeDataResolver,
                                                                    JobDataReplicator jobDataReplicator,
                                                                    ReadOnlyJobOperations jobOperations,
                                                                    EvictionDataReplicator evictionDataReplicator,
                                                                    EvictionServiceClient evictionServiceClient,
                                                                    DeschedulerService deschedulerService,
                                                                    TaskRelocationStore activeStore,
                                                                    TaskRelocationResultStore archiveStore,
                                                                    TitusRuntime titusRuntime) {
        return new DefaultRelocationWorkflowExecutor(
                configuration,
                nodeDataResolver,
                jobDataReplicator,
                jobOperations,
                evictionDataReplicator,
                evictionServiceClient,
                deschedulerService,
                activeStore,
                archiveStore,
                titusRuntime
        );
    }

    @Bean
    public NodeConditionController getNodeConditionCtrl(RelocationConfiguration configuration,
                                                        NodeDataResolver nodeDataResolver,
                                                        JobDataReplicator jobDataReplicator,
                                                        ReadOnlyJobOperations readOnlyJobOperations,
                                                        JobManagementClient jobManagementClient,
                                                        TitusRuntime titusRuntime) {
        return new DefaultNodeConditionController(configuration, nodeDataResolver,
                jobDataReplicator, readOnlyJobOperations, jobManagementClient, titusRuntime);
    }

}
