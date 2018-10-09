package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.tuple.Pair;

import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.common.util.CollectionsExt.transformValues;

class EvacuatedAgentsAllocationTracker {

    private final Map<String, AgentInstance> instancesById;
    private final Map<String, Pair<AgentInstance, List<Task>>> instancesAndTasksById;
    private final Set<String> descheduledTasks = new HashSet<>();

    EvacuatedAgentsAllocationTracker(ReadOnlyAgentOperations agentOperations, Map<String, Task> tasksById) {
        this.instancesById = agentOperations.getInstanceGroups().stream()
                .filter(this::isRemovable)
                .flatMap(ig -> agentOperations.getAgentInstances(ig.getId()).stream())
                .collect(Collectors.toMap(AgentInstance::getId, i -> i));
        this.instancesAndTasksById = transformValues(instancesById, i -> Pair.of(i, findTasksOnInstance(i, tasksById)));
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
        return copyAndRemove(pair.getRight(), t -> !descheduledTasks.contains(t.getId()));
    }

    private boolean isRemovable(AgentInstanceGroup instanceGroup) {
        return instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable;
    }

    private List<Task> findTasksOnInstance(AgentInstance instance, Map<String, Task> tasksById) {
        return tasksById.values().stream()
                .filter(task -> isAssignedToAgent(task) && isOnInstance(instance, task))
                .collect(Collectors.toList());
    }

    private boolean isAssignedToAgent(Task task) {
        TaskState state = task.getStatus().getState();
        return state != TaskState.Accepted && state != TaskState.Finished;
    }

    private boolean isOnInstance(AgentInstance instance, Task task) {
        String taskAgentId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID);
        return taskAgentId != null && taskAgentId.equals(instance.getId());
    }
}
