package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

class TaskMigrationDescheduler {

    private static final double FITNESS_NONE = 0.0;
    private static final double FITNESS_PERFECT = 1.0;

    private static final Pair<Double, List<Task>> FITNESS_RESULT_NONE = Pair.of(FITNESS_NONE, Collections.emptyList());

    private static final int MAX_EXPECTED_AGENT_CPUS = 64;

    /**
     * A factor used to lower a fitness score for agents that cannot be fully evacuated. Total factor is a multiplication
     * of tasks left and this value. We set it to 1/64, as 64 is the maximum number of processors we may have per agent
     * instance. If actual number of CPUs is higher than 64, it is ok. We will just not distinguish agents which are left
     * with more than 64 tasks on them.
     */
    private static final double TASK_ON_AGENT_PENALTY = 1.0 / MAX_EXPECTED_AGENT_CPUS;

    private final Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans;

    private final EvacuatedAgentsAllocationTracker evacuatedAgentsAllocationTracker;
    private final EvictionQuotaTracker evictionQuotaTracker;
    private final Clock clock;

    TaskMigrationDescheduler(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans,
                             EvacuatedAgentsAllocationTracker evacuatedAgentsAllocationTracker,
                             EvictionQuotaTracker evictionQuotaTracker,
                             TitusRuntime titusRuntime) {
        this.plannedAheadTaskRelocationPlans = plannedAheadTaskRelocationPlans;
        this.evacuatedAgentsAllocationTracker = evacuatedAgentsAllocationTracker;
        this.evictionQuotaTracker = evictionQuotaTracker;
        this.clock = titusRuntime.getClock();
    }

    Optional<Pair<AgentInstance, List<Task>>> nextBestMatch() {
        if (evictionQuotaTracker.getSystemEvictionQuota() <= 0) {
            return Optional.empty();
        }

        return evacuatedAgentsAllocationTracker.getInstances().values().stream()
                .map(i -> Pair.of(i, computeFitness(i)))
                .filter(p -> p.getRight().getLeft() > 0)
                .max(Comparator.comparingDouble(p -> p.getRight().getLeft()))
                .map(p -> {
                    AgentInstance agent = p.getLeft();
                    List<Task> tasks = p.getRight().getRight();

                    tasks.forEach(task -> {
                        evacuatedAgentsAllocationTracker.descheduled(task);
                        evictionQuotaTracker.consumeQuota(task.getJobId());
                    });

                    return Pair.of(agent, tasks);
                });
    }

    private Pair<Double, List<Task>> computeFitness(AgentInstance agent) {
        List<Task> tasks = evacuatedAgentsAllocationTracker.getTasksOnAgent(agent.getId());
        if (tasks.isEmpty()) {
            return FITNESS_RESULT_NONE;
        }

        long terminateLimit = Math.min(tasks.size(), evictionQuotaTracker.getSystemEvictionQuota());
        if (terminateLimit <= 0) {
            return FITNESS_RESULT_NONE;
        }

        Map<String, List<Task>> chosen = new HashMap<>();
        List<Task> chosenList = new ArrayList<>();
        for (Task task : tasks) {
            if (canTerminate(task)) {
                String jobId = task.getJobId();
                long quota = evictionQuotaTracker.getJobEvictionQuota(jobId);
                long used = chosen.getOrDefault(jobId, Collections.emptyList()).size();
                if ((quota - used) > 0) {
                    chosen.computeIfAbsent(jobId, jid -> new ArrayList<>()).add(task);
                    chosenList.add(task);
                    if (terminateLimit <= chosenList.size()) {
                        break;
                    }
                }
            }
        }

        if (chosenList.size() == 0) {
            return FITNESS_RESULT_NONE;
        }

        int leftOnAgent = tasks.size() - chosenList.size();
        double fitness = Math.max(FITNESS_PERFECT - leftOnAgent * TASK_ON_AGENT_PENALTY, 0.01);

        return Pair.of(fitness, chosenList);
    }

    private boolean canTerminate(Task task) {
        TaskRelocationPlan relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());

        // If no relocation plan is found, this means the disruption budget policy does not limit us here.
        if (relocationPlan == null) {
            return true;
        }

        return relocationPlan.getRelocationTime() <= clock.wallTime();
    }
}
