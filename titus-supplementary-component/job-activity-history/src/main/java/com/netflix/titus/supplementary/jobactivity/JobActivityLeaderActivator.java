package com.netflix.titus.supplementary.jobactivity;

import java.util.Arrays;
import java.util.List;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationConfiguration;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinator;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationStatus;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityStore;

import static com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinators.coordinatorWithLoggingCallback;
import static com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinators.coordinatorWithSystemExitCallback;

@Singleton
public class JobActivityLeaderActivator implements LeaderActivationStatus {

    private final LeaderActivationCoordinator coordinator;

    @Inject
    public JobActivityLeaderActivator(LeaderActivationConfiguration configuration,
                                      JobActivityStore jobActivityStore,
                                      ClusterMembershipService membershipService,
                                      TitusRuntime titusRuntime) {
        List<LeaderActivationListener> services = Arrays.asList(jobActivityStore);
        this.coordinator = configuration.isSystemExitOnLeadershipLost()
                ? coordinatorWithSystemExitCallback(configuration, services, membershipService, titusRuntime)
                : coordinatorWithLoggingCallback(configuration, services, membershipService, titusRuntime);
    }

    @PreDestroy
    public void shutdown() {
        coordinator.shutdown();
    }

    @Override
    public boolean isActivatedLeader() {
        return coordinator.isActivatedLeader();
    }
}
