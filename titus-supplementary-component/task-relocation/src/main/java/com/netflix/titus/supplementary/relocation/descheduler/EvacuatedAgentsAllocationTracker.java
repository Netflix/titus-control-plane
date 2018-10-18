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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;

import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.common.util.CollectionsExt.transformValues;

class EvacuatedAgentsAllocationTracker {

    private final Map<String, AgentInstance> instancesById;
    private final Map<String, Pair<AgentInstance, List<Task>>> instancesAndTasksById;
    private final Set<String> descheduledTasks = new HashSet<>();

    EvacuatedAgentsAllocationTracker(ReadOnlyAgentOperations agentOperations, Map<String, Task> tasksById) {
        this.instancesById = agentOperations.getInstanceGroups().stream()
                .filter(RelocationUtil::isRemovable)
                .flatMap(ig -> agentOperations.getAgentInstances(ig.getId()).stream())
                .collect(Collectors.toMap(AgentInstance::getId, i -> i));
        this.instancesAndTasksById = transformValues(instancesById, i -> Pair.of(i, RelocationUtil.findTasksOnInstance(i, tasksById.values())));
    }

    Map<String, AgentInstance> getInstances() {
        return instancesById;
    }

    void descheduled(Task task) {
        descheduledTasks.add(task.getId());
    }

    List<Task> getTasksOnAgent(String instanceId) {
        Pair<AgentInstance, List<Task>> pair = Preconditions.checkNotNull(
                instancesAndTasksById.get(instanceId),
                "Agent instance not found: instanceId=%s", instanceId
        );
        return copyAndRemove(pair.getRight(), t -> descheduledTasks.contains(t.getId()));
    }
}
