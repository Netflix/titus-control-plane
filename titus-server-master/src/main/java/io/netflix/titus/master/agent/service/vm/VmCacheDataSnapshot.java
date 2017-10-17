/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.master.agent.service.vm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.common.util.CollectionsExt;

class VmCacheDataSnapshot {

    private List<InstanceGroup> instanceGroups;
    private Map<String, InstanceGroup> instanceGroupMap;

    private Map<String, Instance> instanceMap;

    private VmCacheDataSnapshot(Map<String, InstanceGroup> instanceGroupMap,
                                Map<String, Instance> instanceMap) {
        this.instanceGroups = new ArrayList<>(instanceGroupMap.values());
        this.instanceGroupMap = instanceGroupMap;
        this.instanceMap = instanceMap;
    }

    List<InstanceGroup> getInstanceGroups() {
        return instanceGroups;
    }

    InstanceGroup getServerGroup(String id) {
        return instanceGroupMap.get(id);
    }

    Instance getAgentInstance(String id) {
        return instanceMap.get(id);
    }

    VmCacheDataSnapshot updateServerGroup(InstanceGroup updatedServerGroup) {
        HashMap<String, InstanceGroup> newServerGroupMap = new HashMap<>(instanceGroupMap);
        newServerGroupMap.put(updatedServerGroup.getId(), updatedServerGroup);
        return new VmCacheDataSnapshot(newServerGroupMap, instanceMap);
    }

    VmCacheDataSnapshot updateServers(List<Instance> instances) {
        HashMap<String, Instance> newServerMap = new HashMap<>(instanceMap);
        instances.forEach(updated -> newServerMap.put(updated.getId(), updated));
        return new VmCacheDataSnapshot(instanceGroupMap, newServerMap);
    }

    VmCacheDataSnapshot removeServerGroup(String removedServerGroupId) {
        return new VmCacheDataSnapshot(
                CollectionsExt.copyAndRemove(instanceGroupMap, removedServerGroupId),
                CollectionsExt.copyAndRemoveByValue(instanceMap, instance -> instance.getInstanceGroupId().equals(removedServerGroupId))
        );
    }

    VmCacheDataSnapshot addServerGroups(List<InstanceGroup> newServerGroups) {
        HashMap<String, InstanceGroup> allServerGroups = new HashMap<>(instanceGroupMap);
        newServerGroups.forEach(sg -> allServerGroups.put(sg.getId(), sg));
        return new VmCacheDataSnapshot(allServerGroups, instanceMap);
    }

    static VmCacheDataSnapshot empty() {
        return new VmCacheDataSnapshot(Collections.emptyMap(), Collections.emptyMap());
    }
}
