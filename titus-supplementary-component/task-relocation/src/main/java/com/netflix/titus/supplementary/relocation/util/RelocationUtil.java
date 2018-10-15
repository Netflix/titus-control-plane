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
}
