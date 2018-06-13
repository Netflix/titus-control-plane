package com.netflix.titus.runtime.connector.eviction.replicator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.eviction.model.event.EvictionSnapshotEndEvent;
import com.netflix.titus.api.eviction.model.event.SystemDisruptionBudgetUpdateEvent;
import com.netflix.titus.api.model.reference.TierReference;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class GrpcEvictionReplicatorEventStream implements ReplicatorEventStream<EvictionDataSnapshot> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcEvictionReplicatorEventStream.class);

    private final EvictionServiceClient client;
    private final DataReplicatorMetrics metrics;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public GrpcEvictionReplicatorEventStream(EvictionServiceClient client,
                                             DataReplicatorMetrics metrics,
                                             TitusRuntime titusRuntime,
                                             Scheduler scheduler) {

        this.client = client;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<ReplicatorEvent<EvictionDataSnapshot>> connect() {
        return Observable.fromCallable(CacheUpdater::new)
                .flatMap(cacheUpdater -> client.observeEvents(true).flatMap(cacheUpdater::onEvent))
                .compose(ObservableExt.reemiter(
                        // If there are no events in the stream, we will periodically emit the last cache instance
                        // with the updated cache update timestamp, so it does not look stale.
                        cacheEvent -> new ReplicatorEvent<>(cacheEvent.getData(), titusRuntime.getClock().wallTime()),
                        LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS,
                        scheduler
                ))
                .doOnNext(event -> metrics.event(titusRuntime.getClock().wallTime() - event.getLastUpdateTime()))
                .doOnSubscribe(metrics::connected)
                .doOnUnsubscribe(metrics::disconnected)
                .doOnError(metrics::disconnected)
                .doOnCompleted(metrics::disconnected);
    }

    private class CacheUpdater {

        private final List<EvictionEvent> snapshotEvents = new ArrayList<>();
        private final AtomicReference<EvictionDataSnapshot> lastSnapshotRef = new AtomicReference<>();

        private Observable<ReplicatorEvent<EvictionDataSnapshot>> onEvent(EvictionEvent event) {
            try {
                if (lastSnapshotRef.get() != null) {
                    return processSnapshotUpdate(event);
                }
                if (event instanceof EvictionSnapshotEndEvent) {
                    return buildInitialCache();
                }
                snapshotEvents.add(event);
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the agent change notification: {}", event, e);
                return Observable.error(e); // Return error to force the cache reconnect.
            }
            return Observable.empty();
        }

        private Observable<ReplicatorEvent<EvictionDataSnapshot>> buildInitialCache() {
            SystemDisruptionBudget globalDisruptionBudget = null;
            EvictionQuota globalEvictionQuota = null;
            Map<Tier, SystemDisruptionBudget> tierSystemDisruptionBudgets = new HashMap<>();
            Map<Tier, EvictionQuota> tierEvictionQuotas = new HashMap<>();
            Map<String, SystemDisruptionBudget> capacityGroupSystemDisruptionBudgets = new HashMap<>();
            Map<String, EvictionQuota> capacityGroupEvictionQuotas = new HashMap<>();

            for (EvictionEvent event : snapshotEvents) {
                if (event instanceof SystemDisruptionBudgetUpdateEvent) {
                    SystemDisruptionBudget budget = ((SystemDisruptionBudgetUpdateEvent) event).getSystemDisruptionBudget();
                    switch (budget.getReference().getLevel()) {
                        case Global:
                            globalDisruptionBudget = budget;
                            break;
                        case Tier:
                            tierSystemDisruptionBudgets.put(((TierReference) budget.getReference()).getTier(), budget);
                            break;
                        case CapacityGroup:
                            capacityGroupSystemDisruptionBudgets.put(budget.getReference().getName(), budget);
                            break;
                    }
                } else if (event instanceof EvictionQuotaEvent) {
                    EvictionQuota quota = ((EvictionQuotaEvent) event).getQuota();
                    switch (quota.getReference().getLevel()) {
                        case Global:
                            globalEvictionQuota = quota;
                            break;
                        case Tier:
                            tierEvictionQuotas.put(((TierReference) quota.getReference()).getTier(), quota);
                            break;
                        case CapacityGroup:
                            capacityGroupEvictionQuotas.put(quota.getReference().getName(), quota);
                            break;
                    }
                }
            }

            // Clear so the garbage collector can reclaim the memory (we no longer need this data).
            snapshotEvents.clear();

            checkNotNull(globalDisruptionBudget, "Global disruption budget missing");
            checkNotNull(globalEvictionQuota, "Global eviction quota missing");
            checkState(tierSystemDisruptionBudgets.size() == Tier.values().length, "Tier disruption budgets missing: found=%s", tierSystemDisruptionBudgets);
            checkState(tierEvictionQuotas.size() == Tier.values().length, "Tier eviction quotas missing: found=%s", tierEvictionQuotas);


            EvictionDataSnapshot initialSnapshot = new EvictionDataSnapshot(
                    globalDisruptionBudget,
                    tierSystemDisruptionBudgets,
                    capacityGroupSystemDisruptionBudgets,
                    globalEvictionQuota,
                    tierEvictionQuotas,
                    capacityGroupEvictionQuotas
            );

            lastSnapshotRef.set(initialSnapshot);
            return Observable.just(new ReplicatorEvent<>(initialSnapshot, titusRuntime.getClock().wallTime()));
        }

        private Observable<ReplicatorEvent<EvictionDataSnapshot>> processSnapshotUpdate(EvictionEvent event) {
            EvictionDataSnapshot snapshot = lastSnapshotRef.get();
            Optional<EvictionDataSnapshot> newSnapshot = Optional.empty();

            if (event instanceof SystemDisruptionBudgetUpdateEvent) {
                newSnapshot = snapshot.updateSystemDisruptionBudget(((SystemDisruptionBudgetUpdateEvent) event).getSystemDisruptionBudget());
            } else if (event instanceof EvictionQuotaEvent) {
                newSnapshot = snapshot.updateEvictionQuota(((EvictionQuotaEvent) event).getQuota());
            } // Ignore all other events, as they are not relevant for snapshot

            if (newSnapshot.isPresent()) {
                lastSnapshotRef.set(newSnapshot.get());
                return Observable.just(new ReplicatorEvent<>(newSnapshot.get(), titusRuntime.getClock().wallTime()));
            }
            return Observable.empty();
        }
    }
}
