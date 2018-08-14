package com.netflix.titus.master.supervisor.service.leader;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.LeaderElector;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
public class LeaderElectionOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionOrchestrator.class);

    private static final long MASTER_INITIAL_UPDATE_TIMEOUT_MS = 5_000;
    private static final long MASTER_STATE_UPDATE_TIMEOUT_MS = 5_000;

    private final LocalMasterInstanceResolver localMasterInstanceResolver;
    private final MasterMonitor masterMonitor;
    private final LeaderElector leaderElector;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    private final Subscription localMasterUpdateSubscription;

    @Inject
    public LeaderElectionOrchestrator(LocalMasterInstanceResolver localMasterInstanceResolver,
                                      MasterMonitor masterMonitor,
                                      LeaderElector leaderElector,
                                      TitusRuntime titusRuntime) {
        this(localMasterInstanceResolver,
                masterMonitor,
                leaderElector,
                fetchInitialMasterInstance(localMasterInstanceResolver),
                titusRuntime,
                Schedulers.computation()
        );
    }

    @VisibleForTesting
    LeaderElectionOrchestrator(LocalMasterInstanceResolver localMasterInstanceResolver,
                               MasterMonitor masterMonitor,
                               LeaderElector leaderElector,
                               MasterInstance initial,
                               TitusRuntime titusRuntime,
                               Scheduler scheduler) {
        this.localMasterInstanceResolver = localMasterInstanceResolver;
        this.masterMonitor = masterMonitor;
        this.leaderElector = leaderElector;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;

        // Synchronously initialize first the local MasterInstance, next subscribe to the stream to react to future changes
        checkAndRecordInitialMasterInstance(initial);

        this.localMasterUpdateSubscription = subscribeToLocalMasterUpdateStream();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(localMasterUpdateSubscription);
    }

    private static MasterInstance fetchInitialMasterInstance(LocalMasterInstanceResolver localMasterInstanceResolver) {
        return localMasterInstanceResolver.observeLocalMasterInstanceUpdates()
                .take(1)
                .timeout(MASTER_INITIAL_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS, Schedulers.computation())
                .toBlocking()
                .firstOrDefault(null);
    }

    private void checkAndRecordInitialMasterInstance(MasterInstance initial) {
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
                ObservableExt.mapWithState(masterMonitor.getCurrentMasterInstance(), (change, currentMaster) ->
                        processChange(change, currentMaster)
                                .map(newMasterInstance -> Pair.of(Optional.of(newMasterInstance), newMasterInstance))
                                .orElseGet(() -> Pair.of(Optional.empty(), currentMaster)))
        ).flatMapCompletable(this::updateOwnMasterInstanceIfChanged);

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
                    .withStatus(MasterStatus.newBuilder()
                            .withState(newState)
                            .withReasonCode(MasterStatus.REASON_CODE_NORMAL)
                            .withReasonMessage("Leader activation status change")
                            .withTimestamp(titusRuntime.getClock().wallTime())
                            .build()
                    )
                    .withStatusHistory(CollectionsExt.copyAndAdd(currentMaster.getStatusHistory(), currentMaster.getStatus()))
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

    private Completable updateOwnMasterInstanceIfChanged(Optional<MasterInstance> newMasterOpt) {
        if (!newMasterOpt.isPresent()) {
            return Completable.complete();
        }

        MasterInstance newMasterInstance = newMasterOpt.get();

        updateLeaderElectionState(newMasterInstance);

        return masterMonitor
                .updateOwnMasterInstance(newMasterInstance)
                .timeout(MASTER_STATE_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduler)
                .doOnCompleted(() -> logger.info("Recorded local MasterInstance update: {}", newMasterInstance));
    }
}
