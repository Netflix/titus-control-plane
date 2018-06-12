package com.netflix.titus.supplementary.relocation.master;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.replicator.DefaultAgentDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.replicator.DefaultJobDataReplicator;

public class MasterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AgentDataReplicator.class).to(DefaultAgentDataReplicator.class);
        bind(JobDataReplicator.class).to(DefaultJobDataReplicator.class);
        bind(TitusMasterOperations.class).to(DefaultTitusMasterOperations.class).asEagerSingleton();
    }
}
