package com.netflix.titus.runtime.connector.agent;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.common.util.tuple.Pair;

@Singleton
public class CachedReadOnlyAgentOperations implements ReadOnlyAgentOperations {

    private final AgentDataReplicator replicator;

    @Inject
    public CachedReadOnlyAgentOperations(AgentDataReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public List<AgentInstanceGroup> getInstanceGroups() {
        return replicator.getCurrent().getInstanceGroups();
    }

    @Override
    public Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId) {
        return replicator.getCurrent().findInstanceGroup(instanceGroupId);
    }

    @Override
    public List<AgentInstance> getAgentInstances(String instanceGroupId) {
        return replicator.getCurrent().getInstances(instanceGroupId);
    }

    @Override
    public Optional<AgentInstance> findAgentInstance(String instanceId) {
        return replicator.getCurrent().findInstance(instanceId);
    }

    @Override
    public List<Pair<AgentInstanceGroup, List<AgentInstance>>> findAgentInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> filter) {
        AgentSnapshot snapshot = replicator.getCurrent();
        return snapshot.getInstanceGroups().stream().map(ig -> Pair.of(ig, snapshot.getInstances(ig.getId()))).collect(Collectors.toList());
    }
}
