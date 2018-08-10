package com.netflix.titus.master.supervisor.service.leader;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.service.LeaderActivator;
import com.netflix.titus.master.supervisor.service.LeaderElector;
import rx.Observable;

@Singleton
public class ImmediateLeaderElector implements LeaderElector {

    private final LeaderActivator leaderActivator;

    @Inject
    public ImmediateLeaderElector(LeaderActivator leaderActivator) {
        this.leaderActivator = leaderActivator;
        leaderActivator.becomeLeader();
    }

    @PreDestroy
    public void shutdown() {
        leaderActivator.stopBeingLeader();
    }

    @Override
    public boolean join() {
        return false;
    }

    @Override
    public boolean leaveIfNotLeader() {
        return false;
    }

    @Override
    public Observable<MasterState> awaitElection() {
        return Observable.just(MasterState.LeaderActivated);
    }
}
