package com.netflix.titus.master.supervisor.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.governator.LifecycleManager;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.event.MasterInstanceRemovedEvent;
import com.netflix.titus.master.supervisor.model.event.MasterInstanceUpdateEvent;
import com.netflix.titus.master.supervisor.model.event.SupervisorEvent;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Singleton
public class DefaultSupervisorOperations implements SupervisorOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSupervisorOperations.class);

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 10_000;

    private final LifecycleManager lifecycleManager;
    private final MasterMonitor masterMonitor;
    private final LeaderActivator leaderActivator;
    private final TitusRuntime titusRuntime;

    @Inject
    public DefaultSupervisorOperations(LifecycleManager lifecycleManager,
                                       MasterMonitor masterMonitor,
                                       LeaderActivator leaderActivator,
                                       TitusRuntime titusRuntime) {
        this.lifecycleManager = lifecycleManager;
        this.masterMonitor = masterMonitor;
        this.leaderActivator = leaderActivator;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public List<MasterInstance> getMasterInstances() {
        return masterMonitor.observeMasters().take(1).toBlocking().first();
    }

    @Override
    public Optional<MasterInstance> findMasterInstance(String instanceId) {
        return getMasterInstances().stream()
                .filter(i -> i.getInstanceId().equals(instanceId))
                .findFirst();
    }

    @Override
    public MasterInstance getMasterInstance(String instanceId) {
        return findMasterInstance(instanceId).orElseThrow(() -> SupervisorServiceException.masterInstanceNotFound(instanceId));
    }

    @Override
    public Optional<MasterInstance> findLeader() {
        return getMasterInstances().stream().filter(i -> MasterState.isLeader(i.getStatus().getState())).findFirst();
    }

    @Override
    public Observable<SupervisorEvent> events() {
        return masterMonitor.observeMasters()
                .compose(ObservableExt.mapWithState(Collections.emptyMap(), this::buildEventList))
                .flatMap(Observable::from);
    }

    @Override
    public void stopBeingLeader(CallMetadata callMetadata) {
        if (!leaderActivator.isLeader()) {
            logger.warn("System shutdown requested for non-leader node by: {}", callMetadata);

            titusRuntime.getSystemLogService().submit(SystemLogEvent.newBuilder()
                    .withPriority(SystemLogEvent.Priority.Warn)
                    .withMessage("System shutdown requested for non-leader node; ignoring it")
                    .withComponent(COMPONENT)
                    .withCategory(SystemLogEvent.Category.Transient)
                    .withContext(CallMetadataUtils.asMap(callMetadata))
                    .build()
            );
            return;
        }

        logger.warn("System shutdown requested for the current leader by: {}", callMetadata);
        titusRuntime.getSystemLogService().submit(SystemLogEvent.newBuilder()
                .withPriority(SystemLogEvent.Priority.Warn)
                .withMessage("System shutdown requested for the leader node")
                .withComponent(COMPONENT)
                .withCategory(SystemLogEvent.Category.Transient)
                .withContext(CallMetadataUtils.asMap(callMetadata))
                .build()
        );

        // Make both threads non-daemon so they do not get terminated too early.
        Thread controllerThread = new Thread("ShutdownController") {
            @Override
            public void run() {
                Thread shutdownThread = new Thread("ShutdownExecutor") {
                    @Override
                    public void run() {
                        lifecycleManager.notifyShutdown();
                    }
                };
                shutdownThread.setDaemon(false);
                shutdownThread.start();

                try {
                    shutdownThread.join(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    // IGNORE
                }
                System.exit(-1);
            }
        };
        controllerThread.setDaemon(false);
        controllerThread.start();
    }

    private Pair<List<SupervisorEvent>, Map<String, MasterInstance>> buildEventList(List<MasterInstance> current, Map<String, MasterInstance> state) {
        Map<String, MasterInstance> newState = new HashMap<>();
        current.forEach(instance -> newState.put(instance.getInstanceId(), instance));

        List<SupervisorEvent> events = new ArrayList<>();

        // Removed Masters
        Set<String> removed = CollectionsExt.copyAndRemove(state.keySet(), newState.keySet());
        removed.forEach(id -> events.add(new MasterInstanceRemovedEvent(state.get(id))));

        // Added Masters
        Set<String> added = CollectionsExt.copyAndRemove(newState.keySet(), state.keySet());
        added.forEach(id -> events.add(new MasterInstanceUpdateEvent(newState.get(id))));

        // Compare with the previous state, and build the new one
        Set<String> changeCandidates = CollectionsExt.copyAndRemove(newState.keySet(), added);
        changeCandidates.forEach(id -> {
            if (MasterInstanceFunctions.areDifferent(newState.get(id), state.get(id))) {
                events.add(new MasterInstanceUpdateEvent(newState.get(id)));
            }
        });

        logger.debug("Master instances updated: current={}", current);
        logger.debug("Master instances updated: previous={}", state.values());
        logger.debug("Master instances updated: events={}", events);

        return Pair.of(events, newState);
    }
}
