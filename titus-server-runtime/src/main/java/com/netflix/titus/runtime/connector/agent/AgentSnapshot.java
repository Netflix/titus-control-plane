package com.netflix.titus.runtime.connector.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.copyAndAdd;
import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.common.util.CollectionsExt.copyAndRemoveByValue;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class AgentSnapshot {

    private final Map<String, AgentInstanceGroup> instanceGroupsById;
    private final List<AgentInstanceGroup> instanceGroupList;

    private final Map<String, AgentInstance> instancesById;
    private final List<AgentInstance> instances;
    private final Map<String, List<AgentInstance>> instancesByInstanceGroupId;


    public AgentSnapshot(Map<String, AgentInstanceGroup> instanceGroupsById, Map<String, List<AgentInstance>> instancesByInstanceGroupId) {
        this.instanceGroupsById = instanceGroupsById;
        this.instanceGroupList = Collections.unmodifiableList(new ArrayList<>(instanceGroupsById.values()));
        this.instancesByInstanceGroupId = asUnmodifiable(instancesByInstanceGroupId);
        this.instances = toAllInstances(instancesByInstanceGroupId);
        this.instancesById = toInstanceByIdMap(instances);
    }

    public AgentSnapshot(AgentSnapshot previous, AgentInstanceGroup instanceGroup, boolean remove) {
        if (remove) {
            this.instanceGroupsById = unmodifiableMap(copyAndRemove(previous.instanceGroupsById, instanceGroup.getId()));
            this.instancesById = unmodifiableMap(copyAndRemoveByValue(previous.instancesById, instance -> instance.getInstanceGroupId().equals(instanceGroup.getId())));
            this.instances = unmodifiableList(copyAndRemove(previous.instances, instance -> instance.getInstanceGroupId().equals(instanceGroup.getId())));
            this.instancesByInstanceGroupId = unmodifiableMap(copyAndRemove(previous.instancesByInstanceGroupId, instanceGroup.getId()));
        } else {
            this.instanceGroupsById = unmodifiableMap(copyAndAdd(previous.instanceGroupsById, instanceGroup.getId(), instanceGroup));
            this.instancesById = previous.instancesById;
            this.instances = previous.instances;
            this.instancesByInstanceGroupId = previous.instancesByInstanceGroupId;
        }

        this.instanceGroupList = Collections.unmodifiableList(new ArrayList<>(instanceGroupsById.values()));
    }

    public AgentSnapshot(AgentSnapshot previous, AgentInstance instance, boolean remove) {
        String instanceGroupId = instance.getInstanceGroupId();

        AgentInstanceGroup currentInstanceGroup = previous.instanceGroupsById.get(instanceGroupId);
        Preconditions.checkNotNull(currentInstanceGroup, "Inconsistent data. Agent instance without associated instance group: %s", instanceGroupId);

        if (remove) {
            this.instancesById = unmodifiableMap(copyAndRemove(previous.instancesById, instance.getId()));
            this.instances = unmodifiableList(copyAndRemove(previous.instances, i -> i.getId().equals(instance.getId())));

            List<AgentInstance> groupInstances = copyAndRemove(previous.instancesByInstanceGroupId.getOrDefault(instanceGroupId, Collections.emptyList()), i -> i.getId().equals(instance.getId()));
            this.instancesByInstanceGroupId = unmodifiableMap(copyAndAdd(previous.instancesByInstanceGroupId, instanceGroupId, groupInstances));

        } else {
            this.instancesById = unmodifiableMap(copyAndAdd(previous.instancesById, instance.getId(), instance));
            this.instances = updateInstanceInList(previous.instances, instance);
            List<AgentInstance> groupInstances = updateInstanceInList(previous.instancesByInstanceGroupId.getOrDefault(instance.getId(), Collections.emptyList()), instance);
            this.instancesByInstanceGroupId = unmodifiableMap(copyAndAdd(previous.instancesByInstanceGroupId, instanceGroupId, groupInstances));
        }

        AgentInstanceGroup newInstanceGroup = currentInstanceGroup.toBuilder().withCurrent(instancesByInstanceGroupId.get(instanceGroupId).size()).build();
        this.instanceGroupsById = CollectionsExt.copyAndAdd(previous.instanceGroupsById, instanceGroupId, newInstanceGroup);
        this.instanceGroupList = Collections.unmodifiableList(new ArrayList<>(instanceGroupsById.values()));
    }

    public List<AgentInstanceGroup> getInstanceGroups() {
        return instanceGroupList;
    }

    public Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId) {
        return Optional.ofNullable(instanceGroupsById.get(instanceGroupId));
    }

    public List<AgentInstance> getInstances() {
        return instances;
    }

    public List<AgentInstance> getInstances(String instanceGroupId) {
        return instancesByInstanceGroupId.getOrDefault(instanceGroupId, Collections.emptyList());
    }

    public Optional<AgentInstance> findInstance(String instanceId) {
        return Optional.ofNullable(instancesById.get(instanceId));
    }

    public Optional<AgentSnapshot> updateInstanceGroup(AgentInstanceGroup instanceGroup) {
        return Optional.of(new AgentSnapshot(this, instanceGroup, false));
    }

    public Optional<AgentSnapshot> removeInstanceGroup(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = instanceGroupsById.get(instanceGroupId);
        if (instanceGroup != null) {
            return Optional.of(new AgentSnapshot(this, instanceGroup, true));
        }
        return Optional.empty();
    }

    public Optional<AgentSnapshot> updateInstance(AgentInstance instance) {
        return Optional.of(new AgentSnapshot(this, instance, false));
    }

    public Optional<AgentSnapshot> removeInstance(String instanceId) {
        AgentInstance instance = instancesById.get(instanceId);
        if (instance != null) {
            return Optional.of(new AgentSnapshot(this, instance, true));
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AgentSnapshot{instanceGroups=");

        instanceGroupList.forEach(ig -> sb.append(ig.getId()).append('=').append(ig.getCurrent()).append(','));
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    private List<AgentInstance> toAllInstances(Map<String, List<AgentInstance>> instancesByGroupId) {
        if (instancesByGroupId.isEmpty()) {
            return Collections.emptyList();
        }
        List<AgentInstance> instances = new ArrayList<>();
        instancesByGroupId.forEach((id, instanceList) -> instances.addAll(instanceList));
        return Collections.unmodifiableList(instances);
    }

    private Map<String, AgentInstance> toInstanceByIdMap(List<AgentInstance> instances) {
        if (instances.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, AgentInstance> result = new HashMap<>();
        instances.forEach(instance -> result.put(instance.getId(), instance));
        return unmodifiableMap(result);
    }

    private <V> Map<String, List<V>> asUnmodifiable(Map<String, List<V>> mapOfLists) {
        if (mapOfLists.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<V>> result = new HashMap<>();
        mapOfLists.forEach((key, values) -> result.put(key, Collections.unmodifiableList(values)));
        return unmodifiableMap(result);
    }

    private List<AgentInstance> updateInstanceInList(List<AgentInstance> instances, AgentInstance instance) {
        if (instances.isEmpty()) {
            return Collections.singletonList(instance);
        }

        List<AgentInstance> result = new ArrayList<>();
        instances.forEach(i -> result.add(i.getId().equals(instance.getId()) ? instance : i));
        return unmodifiableList(result);
    }
}
