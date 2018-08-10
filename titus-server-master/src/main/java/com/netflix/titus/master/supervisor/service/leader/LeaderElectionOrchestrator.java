package com.netflix.titus.master.supervisor.service.leader;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.service.LeaderElector;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;

@Singleton
public class LeaderElectionOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionOrchestrator.class);

    private static final long MASTER_INITIAL_UPDATE_TIMEOUT_MS = 5_000;
    private static final long MASTER_STATE_UPDATE_TIMEOUT_MS = 5_000;

    private final LocalMasterInstanceResolver localMasterInstanceResolver;
    private final MasterMonitor masterMonitor;
    private final LeaderElector leaderElector;
    private final TitusRuntime titusRuntime;

    private final Subscription localMasterUpdateSubscription;

    @Inject
    public LeaderElectionOrchestrator(LocalMasterInstanceResolver localMasterInstanceResolver,
                                      MasterMonitor masterMonitor,
                                      LeaderElector leaderElector,
                                      TitusRuntime titusRuntime) {
        this.localMasterInstanceResolver = localMasterInstanceResolver;
        this.masterMonitor = masterMonitor;
        this.leaderElector = leaderElector;
        this.titusRuntime = titusRuntime;

        // Synchronously initialize first the local MasterInstance, next subscribe to the stream to react to future changes
        resolveInitialMasterInstance();

        this.localMasterUpdateSubscription = subscribeToLocalMasterUpdateStream();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(localMasterUpdateSubscription);
    }

    private void resolveInitialMasterInstance() {
        MasterInstance initial = localMasterInstanceResolver.observeLocalMasterInstanceUpdates()
                .take(1)
                .timeout(MASTER_INITIAL_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .toBlocking()
                .firstOrDefault(null);
        if (initial == null) {
            String message = "Couldn't resolve the local MasterInstance data. The result is null";
            titusRuntime.getCodeInvariants().inconsistent(message);
            throw new IllegalStateException(message);
        }

        Throwable error = masterMonitor.updateOwnMasterInstance(initial).get();
        if (error != null) {
            logger.error("Could not record local MasterInstance state update", error);
            throw new IllegalStateException(error);
        }
    }

    private Subscription subscribeToLocalMasterUpdateStream() {
        Observable<?> updateStream = Observable.merge(
                localMasterInstanceResolver.observeLocalMasterInstanceUpdates(),
                leaderElector.awaitElection()
        ).compose(
                ObservableExt.combine(masterMonitor::getCurrentMasterInstance)
        ).flatMapCompletable(
                changeCurrentMasterPair -> {
                    Optional<MasterInstance> changeOpt = processChange(changeCurrentMasterPair.getLeft(), changeCurrentMasterPair.getRight());
                    if (!changeOpt.isPresent()) {
                        return Completable.complete();
                    }

                    MasterInstance newMasterInstance = changeOpt.get();
                    updateLeaderElectionState(newMasterInstance);

                    return masterMonitor
                            .updateOwnMasterInstance(newMasterInstance)
                            .timeout(MASTER_STATE_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                            .doOnCompleted(() -> logger.info("Recorder local MasterInstance update: {}", newMasterInstance));
                }, true, 1
        );

        return titusRuntime.persistentStream(updateStream).subscribe();
    }

    private Optional<MasterInstance> processChange(Object change, MasterInstance currentMaster) {
        MasterState currentState = currentMaster.getStatus().getState();

        // MasterInstance update
        if (change instanceof MasterInstance) {
            if (MasterState.isLeader(currentState)) {
                // If already a leader, ignore further updates
                return Optional.empty();
            }
            return Optional.of((MasterInstance) change);
        }

        // Leader election event
        if (change instanceof MasterState) {
            MasterState newState = (MasterState) change;
            if (!MasterState.isLeader(newState)) {
                titusRuntime.getCodeInvariants().inconsistent("Expected leader election state, but got: %s", newState);
                return Optional.empty();
            }
            if (!MasterState.isAfter(currentState, newState)) {
                titusRuntime.getCodeInvariants().inconsistent("Unexpected leader election state: current=%s, new=%s", currentState, newState);
                return Optional.empty();
            }
            MasterInstance newMasterInstance = currentMaster.toBuilder()
                    .build();
            return Optional.of(newMasterInstance);
        }

        return Optional.empty();
    }

    private void updateLeaderElectionState(MasterInstance newMasterInstance) {
        MasterState state = newMasterInstance.getStatus().getState();
        if (state == MasterState.NonLeader) {
            if (leaderElector.join()) {
                logger.info("Joined leader election process, due to MasterInstance state update: {}", newMasterInstance);
            }
        } else if (state == MasterState.Inactive) {
            if (leaderElector.leaveIfNotLeader()) {
                logger.info("Left leader election process, due to MasterInstance state update: {}", newMasterInstance);
            }
        }
    }
}
