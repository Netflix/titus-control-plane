package com.netflix.titus.runtime.connector.agent.replicator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

public class GrpcAgentReplicatorEventStream implements ReplicatorEventStream<AgentSnapshot> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcAgentReplicatorEventStream.class);

    private final AgentManagementClient client;
    private final DataReplicatorMetrics metrics;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public GrpcAgentReplicatorEventStream(AgentManagementClient client,
                                          DataReplicatorMetrics metrics,
                                          TitusRuntime titusRuntime,
                                          Scheduler scheduler) {
        this.client = client;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<ReplicatorEvent<AgentSnapshot>> connect() {
        return Observable.fromCallable(CacheUpdater::new)
                .flatMap(cacheUpdater -> client.observeAgents().flatMap(cacheUpdater::onEvent))
                .compose(ObservableExt.reemiter(
                        // If there are no events in the stream, we will periodically emit the last cache instance
                        // with the updated cache update timestamp, so it does not look stale.
                        cacheEvent -> new ReplicatorEvent<>(cacheEvent.getData(), titusRuntime.getClock().wallTime()),
                        LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS,
                        scheduler
                ))
                .doOnNext(event -> metrics.event( titusRuntime.getClock().wallTime() - event.getLastUpdateTime()))
                .doOnSubscribe(metrics::connected)
                .doOnUnsubscribe(metrics::disconnected)
                .doOnError(metrics::disconnected)
                .doOnCompleted(metrics::disconnected);
    }

    private class CacheUpdater {

        private final Map<String, AgentChangeEvent> snapshotEvents = new HashMap<>();
        private final AtomicReference<AgentSnapshot> lastAgentSnapshotRef = new AtomicReference<>();

        private Observable<ReplicatorEvent<AgentSnapshot>> onEvent(AgentChangeEvent event) {
            try {
                if (lastAgentSnapshotRef.get() != null) {
                    return processSnapshotUpdate(event);
                }
                if (event.getEventCase() == AgentChangeEvent.EventCase.SNAPSHOTEND) {
                    return buildInitialCache();
                }

                switch (event.getEventCase()) {
                    case INSTANCEGROUPUPDATE:
                        snapshotEvents.put(event.getInstanceGroupUpdate().getInstanceGroup().getId(), event);
                        break;
                    case INSTANCEGROUPREMOVED:
                        snapshotEvents.remove(event.getInstanceGroupRemoved().getInstanceGroupId());
                        break;
                    case AGENTINSTANCEUPDATE:
                        snapshotEvents.put(event.getAgentInstanceUpdate().getInstance().getId(), event);
                        break;
                    case AGENTINSTANCEREMOVED:
                        snapshotEvents.remove(event.getInstanceGroupRemoved().getInstanceGroupId(), event);
                        break;
                }
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the agent change notification: {}", event, e);
                return Observable.error(e); // Return error to force the cache reconnect.
            }
            return Observable.empty();
        }

        private Observable<ReplicatorEvent<AgentSnapshot>> buildInitialCache() {
            Map<String, com.netflix.titus.api.agent.model.AgentInstanceGroup> instanceGroupsById = new HashMap<>();
            Map<String, List<com.netflix.titus.api.agent.model.AgentInstance>> instancesByGroupId = new HashMap<>();

            snapshotEvents.forEach((id, event) -> {
                switch (event.getEventCase()) {
                    case INSTANCEGROUPUPDATE:
                        instanceGroupsById.put(id, GrpcAgentModelConverters.toCoreAgentInstanceGroup(event.getInstanceGroupUpdate().getInstanceGroup()));
                        break;
                    case AGENTINSTANCEUPDATE:
                        com.netflix.titus.api.agent.model.AgentInstance instance = GrpcAgentModelConverters.toCoreAgentInstance(event.getAgentInstanceUpdate().getInstance());
                        instancesByGroupId.computeIfAbsent(instance.getInstanceGroupId(), gid -> new ArrayList<>()).add(instance);
                        break;
                }
            });

            // Clear so the garbage collector can reclaim the memory (we no longer need this data).
            snapshotEvents.clear();

            AgentSnapshot initialSnapshot = new AgentSnapshot(instanceGroupsById, instancesByGroupId);
            lastAgentSnapshotRef.set(initialSnapshot);

            logger.info("Agent snapshot loaded: instanceGroups={}, instances={}", initialSnapshot.getInstanceGroups().size(), initialSnapshot.getInstances().size());

            return Observable.just(new ReplicatorEvent<>(initialSnapshot, titusRuntime.getClock().wallTime()));
        }

        private Observable<ReplicatorEvent<AgentSnapshot>> processSnapshotUpdate(AgentChangeEvent event) {
            AgentSnapshot lastSnapshot = lastAgentSnapshotRef.get();
            Optional<AgentSnapshot> newSnapshot;
            switch (event.getEventCase()) {
                case INSTANCEGROUPUPDATE:
                    newSnapshot = lastSnapshot.updateInstanceGroup(GrpcAgentModelConverters.toCoreAgentInstanceGroup(event.getInstanceGroupUpdate().getInstanceGroup()));
                    break;
                case INSTANCEGROUPREMOVED:
                    newSnapshot = lastSnapshot.removeInstanceGroup(event.getInstanceGroupRemoved().getInstanceGroupId());
                    break;
                case AGENTINSTANCEUPDATE:
                    newSnapshot = lastSnapshot.updateInstance(GrpcAgentModelConverters.toCoreAgentInstance(event.getAgentInstanceUpdate().getInstance()));
                    break;
                case AGENTINSTANCEREMOVED:
                    newSnapshot = lastSnapshot.removeInstance(event.getAgentInstanceRemoved().getInstanceId());
                    break;
                default:
                    newSnapshot = Optional.empty();
            }
            if (newSnapshot.isPresent()) {
                lastAgentSnapshotRef.set(newSnapshot.get());
                return Observable.just(new ReplicatorEvent<>(newSnapshot.get(), titusRuntime.getClock().wallTime()));
            }
            return Observable.empty();
        }
    }
}
