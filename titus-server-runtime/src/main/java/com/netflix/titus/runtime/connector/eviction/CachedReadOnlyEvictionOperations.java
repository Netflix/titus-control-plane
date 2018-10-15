package com.netflix.titus.runtime.connector.eviction;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.model.Tier;
import rx.Observable;

@Singleton
public class CachedReadOnlyEvictionOperations implements ReadOnlyEvictionOperations {

    private final EvictionDataReplicator replicator;

    @Inject
    public CachedReadOnlyEvictionOperations(EvictionDataReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public SystemDisruptionBudget getGlobalDisruptionBudget() {
        return replicator.getCurrent().getGlobalDisruptionBudget();
    }

    @Override
    public SystemDisruptionBudget getTierDisruptionBudget(Tier tier) {
        return replicator.getCurrent().getTierDisruptionBudget(tier);
    }

    @Override
    public SystemDisruptionBudget getCapacityGroupDisruptionBudget(String capacityGroupName) {
        return replicator.getCurrent()
                .findCapacityGroupDisruptionBudget(capacityGroupName)
                .orElseThrow(() -> EvictionException.capacityGroupNotFound(capacityGroupName));
    }

    @Override
    public EvictionQuota getGlobalEvictionQuota() {
        return replicator.getCurrent().getGlobalEvictionQuota();
    }

    @Override
    public EvictionQuota getTierEvictionQuota(Tier tier) {
        return replicator.getCurrent().getTierEvictionQuota(tier);
    }

    @Override
    public EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName) {
        return replicator.getCurrent()
                .findCapacityGroupEvictionQuota(capacityGroupName)
                .orElseThrow(() -> EvictionException.capacityGroupNotFound(capacityGroupName));
    }

    @Override
    public Optional<EvictionQuota> findJobEvictionQuota(String jobId) {
        throw new IllegalStateException("method not implemented yet");
    }

    @Override
    public Observable<EvictionEvent> events(boolean includeSnapshot) {
        throw new IllegalStateException("method not implemented yet");
    }
}
