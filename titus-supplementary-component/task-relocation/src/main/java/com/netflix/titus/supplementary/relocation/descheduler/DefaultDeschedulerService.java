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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.eviction.EvictionConfiguration;
import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.supplementary.relocation.model.DeschedulingFailure;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.supplementary.relocation.util.RelocationPredicates;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.hasDisruptionBudget;

/**
 * WARN This is a simple implementation focused on a single task migration use case.
 */
@Singleton
public class DefaultDeschedulerService implements DeschedulerService {

    private final ReadOnlyJobOperations jobOperations;
    private final ReadOnlyEvictionOperations evictionOperations;
    private final NodeDataResolver nodeDataResolver;

    private final TitusRuntime titusRuntime;
    private final EvictionConfiguration evictionConfiguration;
    private final Clock clock;

    @Inject
    public DefaultDeschedulerService(ReadOnlyJobOperations jobOperations,
                                     ReadOnlyEvictionOperations evictionOperations,
                                     NodeDataResolver nodeDataResolver,
                                     EvictionConfiguration evictionConfiguration,
                                     TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.evictionOperations = evictionOperations;
        this.nodeDataResolver = nodeDataResolver;
        this.evictionConfiguration = evictionConfiguration;
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
        EvacuatedAgentsAllocationTracker evacuatedAgentsAllocationTracker = new EvacuatedAgentsAllocationTracker(nodeDataResolver.resolve(), tasksById);
        EvictionQuotaTracker evictionQuotaTracker = new EvictionQuotaTracker(evictionOperations, jobs);

        TaskMigrationDescheduler taskMigrationDescheduler = new TaskMigrationDescheduler(
                plannedAheadTaskRelocationPlans,
                evacuatedAgentsAllocationTracker,
                evictionQuotaTracker,
                evictionConfiguration,
                jobs, tasksById,
                titusRuntime
        );

        Map<String, DeschedulingResult> requestedImmediateEvictions = taskMigrationDescheduler.findAllImmediateEvictions();
        Map<String, DeschedulingResult> requestedEvictions = taskMigrationDescheduler.findRequestedJobOrTaskMigrations();
        Map<String, DeschedulingResult> allRequestedEvictions = CollectionsExt.merge(requestedImmediateEvictions, requestedEvictions);

        Map<String, DeschedulingResult> regularEvictions = new HashMap<>();
        Optional<Pair<Node, List<Task>>> bestMatch;
        while ((bestMatch = taskMigrationDescheduler.nextBestMatch()).isPresent()) {
            Node agent = bestMatch.get().getLeft();
            List<Task> tasks = bestMatch.get().getRight();
            tasks.forEach(task -> {
                if (!allRequestedEvictions.containsKey(task.getId())) {
                    Optional<TaskRelocationPlan> relocationPlanForTask = getRelocationPlanForTask(agent, task, plannedAheadTaskRelocationPlans);
                    relocationPlanForTask.ifPresent(rp -> regularEvictions.put(
                            task.getId(),
                            DeschedulingResult.newBuilder()
                                    .withTask(task)
                                    .withAgentInstance(agent)
                                    .withTaskRelocationPlan(rp)
                                    .build()
                    ));
                }
            });
        }

        // Find eviction which could not be scheduled now.
        for (Task task : tasksById.values()) {
            if (allRequestedEvictions.containsKey(task.getId()) || regularEvictions.containsKey(task.getId())) {
                continue;
            }
            if (evacuatedAgentsAllocationTracker.isEvacuated(task)) {

                DeschedulingFailure failure = taskMigrationDescheduler.getDeschedulingFailure(task);
                TaskRelocationPlan relocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());

                if (relocationPlan == null) {
                    relocationPlan = newNotDelayedRelocationPlan(task, false);
                }

                Node agent = evacuatedAgentsAllocationTracker.getRemovableAgent(task);
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

    private TaskRelocationPlan newNotDelayedRelocationPlan(Task task, boolean approved) {
        long now = clock.wallTime();
        String reasonMessage = approved
                ? "Enough quota to migrate the task (no migration delay configured)"
                : "Not enough quota to migrate the task (but no migration delay configured)";
        return TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.TaskMigration)
                .withReasonMessage(reasonMessage)
                .withDecisionTime(now)
                .withRelocationTime(now)
                .build();
    }

    @VisibleForTesting
    Optional<TaskRelocationPlan> getRelocationPlanForTask(Node agent, Task task,
                                                          Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans) {
        AtomicReference<Optional<TaskRelocationPlan>> result = new AtomicReference<>(Optional.empty());
        TaskRelocationPlan plannedAheadTaskRelocationPlan = plannedAheadTaskRelocationPlans.get(task.getId());
        if (plannedAheadTaskRelocationPlan == null) {
            // recheck if a self managed plan is needed
            jobOperations.getJob(task.getJobId()).ifPresent(job ->
                    RelocationPredicates.checkIfNeedsRelocationPlan(job, task, agent).ifPresent(reason -> {
                        if (RelocationPredicates.isSelfManaged(job) && hasDisruptionBudget(job)) {
                            result.set(Optional.of(RelocationUtil.buildSelfManagedRelocationPlan(job, task, reason, clock.wallTime())));
                        }
                    }));

            if (!result.get().isPresent()) {
                result.set(Optional.of(newNotDelayedRelocationPlan(task, true)));
            }
        } else {
            result.set(Optional.of(plannedAheadTaskRelocationPlan));
        }
        return result.get();
    }
}
