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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.DeschedulingFailure;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.supplementary.relocation.util.RelocationPredicates;

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
    private final Map<String, Job<?>> jobsById;
    private final Map<String, Task> tasksById;
    private final Clock clock;

    TaskMigrationDescheduler(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans,
                             EvacuatedAgentsAllocationTracker evacuatedAgentsAllocationTracker,
                             EvictionQuotaTracker evictionQuotaTracker,
                             Map<String, Job<?>> jobsById,
                             Map<String, Task> tasksById,
                             TitusRuntime titusRuntime) {
        this.plannedAheadTaskRelocationPlans = plannedAheadTaskRelocationPlans;
        this.evacuatedAgentsAllocationTracker = evacuatedAgentsAllocationTracker;
        this.evictionQuotaTracker = evictionQuotaTracker;
        this.jobsById = jobsById;
        this.tasksById = tasksById;
        this.clock = titusRuntime.getClock();
    }

    Map<String, DeschedulingResult> findAllImmediateEvictions() {
        Map<String, DeschedulingResult> result = new HashMap<>();
        tasksById.values().forEach(task -> {
            Job<?> job = jobsById.get(task.getJobId());
            AgentInstance instance = evacuatedAgentsAllocationTracker.getAgent(task);
            if (job != null && instance != null) {
                RelocationPredicates.checkIfMustBeRelocatedImmediately(job, task, instance).ifPresent(reason -> {
                    TaskRelocationPlan plan = TaskRelocationPlan.newBuilder()
                            .withTaskId(task.getId())
                            .withReason(TaskRelocationPlan.TaskRelocationReason.TaskMigration)
                            .withReasonMessage(reason)
                            .withRelocationTime(clock.wallTime())
                            .build();

                    DeschedulingResult deschedulingResult = DeschedulingResult.newBuilder()
                            .withTask(task)
                            .withAgentInstance(instance)
                            .withTaskRelocationPlan(plan)
                            .build();

                    result.put(task.getId(), deschedulingResult);
                });
            }
        });
        return result;
    }

    Optional<Pair<AgentInstance, List<Task>>> nextBestMatch() {
        if (evictionQuotaTracker.getSystemEvictionQuota() <= 0) {
            return Optional.empty();
        }

        return evacuatedAgentsAllocationTracker.getRemovableAgentsById().values().stream()
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

    DeschedulingFailure getDeschedulingFailure(Task task) {
        Job<?> job = jobsById.get(task.getJobId());

        String message;
        if (job == null) {
            message = "No job record found";
        } else {
            AgentInstance instance = evacuatedAgentsAllocationTracker.getAgent(task);
            Optional<String> blockedOpt = instance != null
                    ? RelocationPredicates.checkIfRelocationBlocked(job, task, instance)
                    : Optional.empty();

            if (blockedOpt.isPresent()) {
                message = blockedOpt.get();
            } else if (!canTerminate(task)) {
                message = "Migration deadline not reached yet";
            } else if (evictionQuotaTracker.getJobEvictionQuota(job.getId()) <= 0) {
                message = "Not enough job quota";
            } else {
                message = "Unknown";
            }
        }

        return DeschedulingFailure.newBuilder().withReasonMessage(message).build();
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
        Job<?> job = jobsById.get(task.getJobId());
        if (job == null || DisruptionBudgetFunctions.isLegacyJob(job)) {
            return false;
        }

        TaskRelocationPlan relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());

        // If no relocation plan is found, this means the disruption budget policy does not limit us here.
        if (relocationPlan == null) {
            return true;
        }

        return relocationPlan.getRelocationTime() <= clock.wallTime();
    }
}
