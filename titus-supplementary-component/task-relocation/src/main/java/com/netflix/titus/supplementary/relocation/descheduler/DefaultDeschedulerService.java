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
import java.util.HashMap;
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
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.DeschedulingFailure;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;

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
                plannedAheadTaskRelocationPlans, evacuatedAgentsAllocationTracker, evictionQuotaTracker, jobs, tasksById, titusRuntime
        );

        Map<String, DeschedulingResult> requestedImmediateEvictions = taskMigrationDescheduler.findAllImmediateEvictions();
        Map<String, DeschedulingResult> requestedEvictions = taskMigrationDescheduler.findRequestedJobOrTaskMigrations();
        Map<String, DeschedulingResult> allRequestedEvictions = CollectionsExt.merge(requestedImmediateEvictions, requestedEvictions);

        Map<String, DeschedulingResult> regularEvictions = new HashMap<>();
        Optional<Pair<AgentInstance, List<Task>>> bestMatch;
        while ((bestMatch = taskMigrationDescheduler.nextBestMatch()).isPresent()) {
            AgentInstance agent = bestMatch.get().getLeft();
            List<Task> tasks = bestMatch.get().getRight();
            tasks.forEach(task -> {
                if (!allRequestedEvictions.containsKey(task.getId())) {
                    TaskRelocationPlan relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());
                    if (relocationPlan == null) {
                        relocationPlan = newImmediateRelocationPlan(task);
                    }
                    regularEvictions.put(
                            task.getId(),
                            DeschedulingResult.newBuilder()
                                    .withTask(task)
                                    .withAgentInstance(agent)
                                    .withTaskRelocationPlan(relocationPlan)
                                    .build()
                    );
                }
            });
        }

        // Find eviction which could not be scheduled now.
        for (Task task : tasksById.values()) {
            if (allRequestedEvictions.containsKey(task.getId()) || regularEvictions.containsKey(task.getId())) {
                continue;
            }
            if (evacuatedAgentsAllocationTracker.isEvacuated(task)) {

                TaskRelocationPlan relocationPlan;
                DeschedulingFailure failure;

                if (isManagedByTaskRelocationService(task)) {
                    failure = taskMigrationDescheduler.getDeschedulingFailure(task);
                    relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());
                    if (relocationPlan == null) {
                        relocationPlan = newImmediateRelocationPlan(task);
                    }
                } else {
                    failure = DeschedulingFailure.legacyJobDeschedulingFailure();
                    relocationPlan = newLegacyRelocationPlan(task);
                }

                AgentInstance agent = evacuatedAgentsAllocationTracker.getRemovableAgent(task);
                regularEvictions.put(
                        task.getId(),
                        DeschedulingResult.newBuilder()
                                .withTask(task)
                                .withAgentInstance(agent)
                                .withTaskRelocationPlan(relocationPlan)
                                .withFailure(failure)
                                .build()
                );
            }
        }

        return CollectionsExt.merge(new ArrayList<>(allRequestedEvictions.values()), new ArrayList<>(regularEvictions.values()));
    }

    private boolean isManagedByTaskRelocationService(Task task) {
        return jobOperations.getJob(task.getJobId()).map(JobFunctions::hasDisruptionBudget).orElse(false);
    }

    private TaskRelocationPlan newImmediateRelocationPlan(Task task) {
        long now = clock.wallTime();
        return TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.TaskMigration)
                .withReasonMessage("Immediate task migration, as no migration constraint defined for the job")
                .withDecisionTime(now)
                .withRelocationTime(now)
                .build();
    }

    private TaskRelocationPlan newLegacyRelocationPlan(Task task) {
        long now = clock.wallTime();
        return TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.TaskMigration)
                .withReasonMessage("Attempted failed migration of a legacy job")
                .withDecisionTime(now)
                .withRelocationTime(now)
                .build();
    }
}
