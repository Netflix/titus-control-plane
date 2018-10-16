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

package com.netflix.titus.supplementary.relocation.startup;

import com.google.inject.AbstractModule;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.CachedReadOnlyAgentOperations;
import com.netflix.titus.runtime.connector.agent.replicator.AgentDataReplicatorProvider;
import com.netflix.titus.runtime.connector.eviction.CachedReadOnlyEvictionOperations;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.replicator.EvictionDataReplicatorProvider;
import com.netflix.titus.runtime.connector.jobmanager.CachedReadOnlyJobOperations;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;

public class MasterDataReplicationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(AgentDataReplicator.class).toProvider(AgentDataReplicatorProvider.class);
        bind(ReadOnlyAgentOperations.class).to(CachedReadOnlyAgentOperations.class);

        bind(JobDataReplicator.class).toProvider(JobDataReplicatorProvider.class);
        bind(ReadOnlyJobOperations.class).to(CachedReadOnlyJobOperations.class);

        bind(EvictionDataReplicator.class).toProvider(EvictionDataReplicatorProvider.class);
        bind(ReadOnlyEvictionOperations.class).to(CachedReadOnlyEvictionOperations.class);
    }
}
