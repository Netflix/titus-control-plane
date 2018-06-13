package com.netflix.titus.supplementary.relocation.startup;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.replicator.DefaultAgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.replicator.DefaultEvictionDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.replicator.DefaultJobDataReplicator;
import com.netflix.titus.supplementary.relocation.evacuation.AgentInstanceEvacuator;

public class TaskRelocationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AgentDataReplicator.class).to(DefaultAgentDataReplicator.class);
        bind(JobDataReplicator.class).to(DefaultJobDataReplicator.class);
        bind(EvictionDataReplicator.class).to(DefaultEvictionDataReplicator.class);

        bind(AgentInstanceEvacuator.class).asEagerSingleton();
    }
}
