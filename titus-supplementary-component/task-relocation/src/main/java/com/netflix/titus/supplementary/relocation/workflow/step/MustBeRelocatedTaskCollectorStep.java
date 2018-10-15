package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;

import static com.netflix.titus.supplementary.relocation.util.RelocationUtil.getAgentInstances;
import static com.netflix.titus.supplementary.relocation.util.RelocationUtil.getRemovableGroups;

/**
 * Step at which all containers that are requested to terminate are identified, and their relocation timestamps are set.
 */
public class MustBeRelocatedTaskCollectorStep {

    private final ReadOnlyAgentOperations agentOperations;
    private final ReadOnlyJobOperations jobOperations;
    private final Clock clock;

    public MustBeRelocatedTaskCollectorStep(ReadOnlyAgentOperations agentOperations,
                                            ReadOnlyJobOperations jobOperations,
                                            TitusRuntime titusRuntime) {
        this.agentOperations = agentOperations;
        this.jobOperations = jobOperations;
        this.clock = titusRuntime.getClock();
    }

    public Map<String, TaskRelocationPlan> collectTasksThatMustBeRelocated() {
        List<Task> tasks = jobOperations.getTasks();

        List<AgentInstance> phasedOutAgents = getAgentInstances(agentOperations, getRemovableGroups(agentOperations));

        List<Pair<AgentInstance, Task>> tasksOnPhasedOutAgents = phasedOutAgents.stream()
                .flatMap(agent -> RelocationUtil.findTasksOnInstance(agent, tasks).stream().map(t -> Pair.of(agent, t)))
                .collect(Collectors.toList());

        long now = clock.wallTime();

        return tasksOnPhasedOutAgents.stream()
                .map(agentTaskPair -> {

                    AgentInstance agent = agentTaskPair.getLeft();
                    Task task = agentTaskPair.getRight();

                    return TaskRelocationPlan.newBuilder()
                            .withTaskId(task.getId())
                            .withReason(TaskRelocationPlan.TaskRelocationReason.TaskMigration)
                            .withReasonMessage("Agent instance in PhasedOutState: instanceId=" + agent.getId())
                            .withRelocationTime(now)
                            .build();
                })
                .collect(Collectors.toMap(TaskRelocationPlan::getTaskId, p -> p));
    }
}
