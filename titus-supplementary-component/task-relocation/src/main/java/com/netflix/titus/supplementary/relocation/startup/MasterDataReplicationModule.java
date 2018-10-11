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
