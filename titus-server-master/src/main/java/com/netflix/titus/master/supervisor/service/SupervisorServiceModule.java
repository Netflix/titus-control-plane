package com.netflix.titus.master.supervisor.service;

import com.google.inject.AbstractModule;
import com.netflix.titus.master.supervisor.service.leader.GuiceLeaderActivator;
import com.netflix.titus.master.supervisor.service.leader.ImmediateLeaderElector;
import com.netflix.titus.master.supervisor.service.leader.ImmediateLocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.leader.LeaderElectionOrchestrator;
import com.netflix.titus.master.supervisor.service.leader.LocalMasterMonitor;

public class SupervisorServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalMasterInstanceResolver.class).to(ImmediateLocalMasterInstanceResolver.class);
        bind(MasterMonitor.class).to(LocalMasterMonitor.class);
        bind(LeaderElector.class).to(ImmediateLeaderElector.class).asEagerSingleton();
        bind(LeaderActivator.class).to(GuiceLeaderActivator.class);
        bind(LeaderElectionOrchestrator.class).asEagerSingleton();
        bind(SupervisorOperations.class).to(DefaultSupervisorOperations.class);
    }
}
