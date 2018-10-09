package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan.TaskRelocationReason;

/**
 * WARN This is a simple implementation focused on a single task migration use case.
 */
@Singleton
public class DefaultDeschedulerService implements DeschedulerService {

    private final ReadOnlyJobOperations jobOperations;
    private final ReadOnlyEvictionOperations evictionOperations;
    private final ReadOnlyAgentOperations agentOperations;

    private final TitusRuntime titusRuntime;
    private final Clock clock;

    @Inject
    public DefaultDeschedulerService(ReadOnlyJobOperations jobOperations,
                                     ReadOnlyEvictionOperations evictionOperations,
                                     ReadOnlyAgentOperations agentOperations,
                                     TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.evictionOperations = evictionOperations;
        this.agentOperations = agentOperations;
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;
    }

    @Override
    public List<DeschedulingResult> deschedule(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans) {
        List<Pair<Job, List<Task>>> allJobsAndTasks = jobOperations.getJobsAndTasks();
        Map<String, Job<?>> jobs = allJobsAndTasks.stream().map(Pair::getLeft).collect(Collectors.toMap(Job::getId, j -> j));
        Map<String, Task> tasksById = allJobsAndTasks.stream()
                .flatMap(p -> p.getRight().stream())
                .collect(Collectors.toMap(Task::getId, t -> t));

        EvacuatedAgentsAllocationTracker evacuatedAgentsAllocationTracker = new EvacuatedAgentsAllocationTracker(agentOperations, tasksById);
        EvictionQuotaTracker evictionQuotaTracker = new EvictionQuotaTracker(evictionOperations, jobs);

        TaskMigrationDescheduler taskMigrationDescheduler = new TaskMigrationDescheduler(
                plannedAheadTaskRelocationPlans, evacuatedAgentsAllocationTracker, evictionQuotaTracker, titusRuntime
        );

        List<DeschedulingResult> result = new ArrayList<>();

        Optional<Pair<AgentInstance, List<Task>>> bestMatch;
        while ((bestMatch = taskMigrationDescheduler.nextBestMatch()).isPresent()) {
            AgentInstance agent = bestMatch.get().getLeft();
            List<Task> tasks = bestMatch.get().getRight();
            tasks.forEach(task -> {
                TaskRelocationPlan relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());
                if (relocationPlan == null) {
                    relocationPlan = newImmediateRelocationPlan(task);
                }
                result.add(DeschedulingResult.newBuilder()
                        .withTask(task)
                        .withAgentInstance(agent)
                        .withTaskRelocationPlan(relocationPlan)
                        .build()
                );
            });
        }

        return result;
    }

    private TaskRelocationPlan newImmediateRelocationPlan(Task task) {
        return TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.TaskMigration)
                .withReasonMessage("Immediate task migration, as no migration constraint defined for the job")
                .withRelocationTime(clock.wallTime())
                .build();
    }
}
