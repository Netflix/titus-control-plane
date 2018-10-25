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

package com.netflix.titus.supplementary.relocation.util;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

public final class RelocationUtil {

    public static List<AgentInstance> getAgentInstances(ReadOnlyAgentOperations agentOperations,
                                                        List<AgentInstanceGroup> instanceGroups) {
        return instanceGroups.stream()
                .flatMap(ig -> agentOperations.getAgentInstances(ig.getId()).stream())
                .collect(Collectors.toList());
    }

    public static boolean isRemovable(AgentInstanceGroup instanceGroup) {
        return instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable;
    }

    public static List<AgentInstanceGroup> getRemovableGroups(ReadOnlyAgentOperations agentOperations) {
        return agentOperations.getInstanceGroups().stream()
                .filter(ig -> ig.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable)
                .collect(Collectors.toList());
    }

    public static List<Task> findTasksOnInstance(AgentInstance instance, Collection<Task> tasks) {
        return tasks.stream()
                .filter(task -> isAssignedToAgent(task) && isOnInstance(instance, task))
                .collect(Collectors.toList());
    }

    public static boolean isAssignedToAgent(Task task) {
        TaskState state = task.getStatus().getState();
        return state != TaskState.Accepted && state != TaskState.Finished;
    }

    public static boolean isOnInstance(AgentInstance instance, Task task) {
        String taskAgentId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID);
        return taskAgentId != null && taskAgentId.equals(instance.getId());
    }

    public static String doFormat(TaskRelocationPlan plan) {
        return String.format("{reason=%s, reasonMessage='%s', relocationAfter=%s}", plan.getReason(), plan.getReasonMessage(), DateTimeExt.toUtcDateTimeString(plan.getRelocationTime()));
    }
}
