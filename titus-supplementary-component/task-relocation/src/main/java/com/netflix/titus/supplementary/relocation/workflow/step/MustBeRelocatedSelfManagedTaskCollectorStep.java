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

package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Triple;
import com.netflix.titus.supplementary.relocation.connector.TitusNode;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.relocation.model.RelocationFunctions.areEqualExceptRelocationTime;
import static com.netflix.titus.supplementary.relocation.util.RelocationPredicates.checkIfNeedsRelocationPlan;

/**
 * Step at which all self managed containers that are requested to terminate are identified,
 * and their relocation timestamps are set.
 */
public class MustBeRelocatedSelfManagedTaskCollectorStep {

    private static final Logger logger = LoggerFactory.getLogger(MustBeRelocatedSelfManagedTaskCollectorStep.class);

    private final NodeDataResolver nodeDataResolver;
    private final ReadOnlyJobOperations jobOperations;
    private final StepMetrics metrics;
    private final Clock clock;

    private Map<String, TaskRelocationPlan> lastResult = Collections.emptyMap();

    public MustBeRelocatedSelfManagedTaskCollectorStep(NodeDataResolver nodeDataResolver,
                                                       ReadOnlyJobOperations jobOperations,
                                                       TitusRuntime titusRuntime) {
        this.nodeDataResolver = nodeDataResolver;
        this.jobOperations = jobOperations;
        this.clock = titusRuntime.getClock();
        this.metrics = new StepMetrics("mustBeRelocatedTaskCollectorStep", titusRuntime);
    }

    public Map<String, TaskRelocationPlan> collectTasksThatMustBeRelocated() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Map<String, TaskRelocationPlan> result = buildRelocationPlans();
            metrics.onSuccess(result.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            logger.debug("Step results: {}", result);
            return result;
        } catch (Exception e) {
            logger.error("Step processing error", e);
            metrics.onError(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private Map<String, TaskRelocationPlan> buildRelocationPlans() {
        Map<String, TitusNode> nodes = nodeDataResolver.resolve();
        List<Triple<Job<?>, Task, TitusNode>> allItems = findAllJobTaskAgentTriples(nodes);

        Map<String, TaskRelocationPlan> result = new HashMap<>();

        logger.debug("Number of triplets to check: {}", allItems.size());

        allItems.forEach(triple -> {

            Job<?> job = triple.getFirst();
            Task task = triple.getSecond();
            TitusNode instance = triple.getThird();

            checkIfNeedsRelocationPlan(job, task, instance).ifPresent(reason ->
                    result.put(task.getId(), buildSelfManagedRelocationPlan(job, task, reason))
            );
        });

        this.lastResult = result;

        return result;
    }

    private List<Triple<Job<?>, Task, TitusNode>> findAllJobTaskAgentTriples(Map<String, TitusNode> nodes) {
        Map<String, TitusNode> taskToInstanceMap = RelocationUtil.buildTasksToInstanceMap(nodes, jobOperations);

        List<Triple<Job<?>, Task, TitusNode>> result = new ArrayList<>();
        jobOperations.getJobs().forEach(job -> {
            jobOperations.getTasks(job.getId()).forEach(task -> {
                TaskState taskState = task.getStatus().getState();
                if (taskState == TaskState.StartInitiated || taskState == TaskState.Started) {
                    TitusNode instance = taskToInstanceMap.get(task.getId());
                    if (instance != null) {
                        result.add(Triple.of(job, task, instance));
                    } else {
                        logger.debug("Task in active state with no agent instance: taskId={}, state={}", task.getId(), task.getStatus().getState());
                    }
                }
            });
        });
        return result;
    }

    /**
     * Relocation plans today are limited to self managed polices.
     */
    private TaskRelocationPlan buildSelfManagedRelocationPlan(Job<?> job, Task task, String reason) {
        long now = clock.wallTime();

        SelfManagedDisruptionBudgetPolicy selfManaged = (SelfManagedDisruptionBudgetPolicy) job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();

        TaskRelocationPlan relocationPlan = TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.SelfManagedMigration)
                .withReasonMessage(reason)
                .withDecisionTime(now)
                .withRelocationTime(now + selfManaged.getRelocationTimeMs())
                .build();

        TaskRelocationPlan previous = lastResult.get(task.getId());
        boolean keepPrevious = previous != null &&
                (areEqualExceptRelocationTime(previous, relocationPlan) || previous.getRelocationTime() < relocationPlan.getRelocationTime());

        return keepPrevious ? previous : relocationPlan;
    }
}
