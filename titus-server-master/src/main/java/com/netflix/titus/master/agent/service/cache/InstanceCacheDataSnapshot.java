/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.agent.service.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.common.util.CollectionsExt;

class InstanceCacheDataSnapshot {

    private List<InstanceGroup> instanceGroups;
    private Map<String, InstanceGroup> instanceGroupMap;

    private Map<String, Instance> instanceMap;

    private InstanceCacheDataSnapshot(Map<String, InstanceGroup> instanceGroupMap,
                                      Map<String, Instance> instanceMap) {
        this.instanceGroups = new ArrayList<>(instanceGroupMap.values());
        this.instanceGroupMap = instanceGroupMap;
        this.instanceMap = instanceMap;
    }

    List<InstanceGroup> getInstanceGroups() {
        return instanceGroups;
    }

    InstanceGroup getInstanceGroup(String id) {
        return instanceGroupMap.get(id);
    }

    Instance getAgentInstance(String id) {
        return instanceMap.get(id);
    }

    InstanceCacheDataSnapshot updateInstanceGroup(InstanceGroup updatedInstanceGroup) {
        HashMap<String, InstanceGroup> newInstanceGroupMap = new HashMap<>(instanceGroupMap);
        newInstanceGroupMap.put(updatedInstanceGroup.getId(), updatedInstanceGroup);
        return new InstanceCacheDataSnapshot(newInstanceGroupMap, instanceMap);
    }

    InstanceCacheDataSnapshot updateInstances(List<Instance> instances) {
        HashMap<String, Instance> newInstanceMap = new HashMap<>(instanceMap);
        instances.forEach(updated -> newInstanceMap.put(updated.getId(), updated));
        return new InstanceCacheDataSnapshot(instanceGroupMap, newInstanceMap);
    }

    InstanceCacheDataSnapshot removeInstanceGroup(String removedInstanceGroupId) {
        return new InstanceCacheDataSnapshot(
                CollectionsExt.copyAndRemove(instanceGroupMap, removedInstanceGroupId),
                CollectionsExt.copyAndRemoveByValue(instanceMap, instance -> instance.getInstanceGroupId().equals(removedInstanceGroupId))
        );
    }

    InstanceCacheDataSnapshot addInstanceGroups(List<InstanceGroup> newInstanceGroups) {
        HashMap<String, InstanceGroup> allInstanceGroups = new HashMap<>(instanceGroupMap);
        newInstanceGroups.forEach(instanceGroup -> allInstanceGroups.put(instanceGroup.getId(), instanceGroup));
        return new InstanceCacheDataSnapshot(allInstanceGroups, instanceMap);
    }

    static InstanceCacheDataSnapshot empty() {
        return new InstanceCacheDataSnapshot(Collections.emptyMap(), Collections.emptyMap());
    }
}
