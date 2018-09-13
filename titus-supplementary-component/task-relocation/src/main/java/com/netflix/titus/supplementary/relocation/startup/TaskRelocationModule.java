package com.netflix.titus.supplementary.relocation.startup;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.replicator.AgentDataReplicatorProvider;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.replicator.EvictionDataReplicatorProvider;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import com.netflix.titus.supplementary.relocation.evacuation.AgentInstanceEvacuator;

public class TaskRelocationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(AgentDataReplicator.class).toProvider(AgentDataReplicatorProvider.class);
        bind(JobDataReplicator.class).toProvider(JobDataReplicatorProvider.class);
        bind(EvictionDataReplicator.class).toProvider(EvictionDataReplicatorProvider.class);

        bind(AgentInstanceEvacuator.class).asEagerSingleton();
    }
}
