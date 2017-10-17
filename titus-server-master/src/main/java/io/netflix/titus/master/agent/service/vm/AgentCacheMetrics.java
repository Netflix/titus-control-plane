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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.MetricConstants;

class AgentCacheMetrics {

    private final Registry registry;

    private final Map<String, InstanceGroupMetrics> instanceGroupMetrics = new HashMap<>();
    private final Map<String, InstanceMetrics> instanceMetrics = new HashMap<>();

    AgentCacheMetrics(Registry registry) {
        this.registry = registry;
    }

    void refresh(AgentDataSnapshot snapshot) {
        refreshInstanceGroupMetrics(snapshot);
        refreshInstanceMetrics(snapshot);
    }

    private void refreshInstanceGroupMetrics(AgentDataSnapshot snapshot) {
        Set<String> foundIds = new HashSet<>();
        snapshot.getInstanceGroups().forEach(g -> {
            foundIds.add(g.getId());
            InstanceGroupMetrics current = this.instanceGroupMetrics.get(g.getId());
            if (current == null) {
                instanceGroupMetrics.put(g.getId(), new InstanceGroupMetrics(g));
            } else {
                instanceGroupMetrics.put(g.getId(), current.apply(g));
            }
        });
        CollectionsExt.copyAndRemove(instanceGroupMetrics.keySet(), foundIds).forEach(m ->
                instanceGroupMetrics.remove(m).remove()
        );
    }

    private void refreshInstanceMetrics(AgentDataSnapshot snapshot) {
        Set<String> foundIds = new HashSet<>();
        snapshot.getInstanceGroups().forEach(
                g -> snapshot.getInstances(g.getId())
                        .forEach(i -> {
                            foundIds.add(i.getId());
                            InstanceMetrics current = this.instanceMetrics.get(i.getId());
                            if (current == null) {
                                instanceMetrics.put(i.getId(), new InstanceMetrics(g, i));
                            } else {
                                instanceMetrics.put(i.getId(), current.apply(g, i));
                            }
                        })
        );
        CollectionsExt.copyAndRemove(instanceMetrics.keySet(), foundIds).forEach(m ->
                instanceMetrics.remove(m).remove()
        );
    }

    private class InstanceGroupMetrics {

        private final AgentInstanceGroup instanceGroup;
        private final AtomicInteger counter;

        private InstanceGroupMetrics(AgentInstanceGroup instanceGroup) {
            this.instanceGroup = instanceGroup;
            Id id = registry.createId(
                    MetricConstants.METRIC_AGENT + "instanceGroup",
                    "id", instanceGroup.getId(),
                    "instanceType", instanceGroup.getInstanceType(),
                    "state", instanceGroup.getLifecycleStatus().getState().name(),
                    "tier", instanceGroup.getTier().name()
            );
            this.counter = registry.gauge(id, new AtomicInteger());
            counter.set(1);
        }

        InstanceGroupMetrics apply(AgentInstanceGroup newInstanceGroup) {
            if (newInstanceGroup.getLifecycleStatus().getState() == instanceGroup.getLifecycleStatus().getState()
                    && newInstanceGroup.getTier() == instanceGroup.getTier()) {
                return this;
            }
            remove();
            return new InstanceGroupMetrics(newInstanceGroup);
        }

        void remove() {
            counter.set(0);
        }
    }

    private class InstanceMetrics {

        private final AgentInstanceGroup instanceGroup;
        private final AgentInstance instance;
        private final AtomicInteger counter;

        private InstanceMetrics(AgentInstanceGroup instanceGroup, AgentInstance instance) {
            this.instanceGroup = instanceGroup;
            this.instance = instance;

            Id id = registry.createId(
                    MetricConstants.METRIC_AGENT + "instance",
                    "id", instance.getId(),
                    "instanceGroupId", instance.getInstanceGroupId(),
                    "state", instance.getLifecycleStatus().getState().name(),
                    "instanceType", instanceGroup.getInstanceType(),
                    "tier", instanceGroup.getTier().name()
            );
            this.counter = registry.gauge(id, new AtomicInteger());
            counter.set(1);
        }

        private InstanceMetrics apply(AgentInstanceGroup newInstanceGroup, AgentInstance newInstance) {
            if (newInstance.getLifecycleStatus().getState() == instance.getLifecycleStatus().getState()
                    && newInstanceGroup.getTier() == instanceGroup.getTier()) {
                return this;
            }
            remove();
            return new InstanceMetrics(newInstanceGroup, newInstance);
        }

        private void remove() {
            counter.set(0);
        }
    }
}
