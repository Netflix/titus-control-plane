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

package com.netflix.titus.runtime.connector.agent;

import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.agent.replicator.AgentDataReplicatorProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AgentManagementDataReplicationComponent {

    @Bean
    public AgentDataReplicator getAgentDataReplicator(AgentManagementClient client, TitusRuntime titusRuntime) {
        return new AgentDataReplicatorProvider(client, titusRuntime).get();
    }

    @Bean
    public ReadOnlyAgentOperations getReadOnlyAgentOperations(AgentDataReplicator replicator) {
        return new CachedReadOnlyAgentOperations(replicator);
    }
}
