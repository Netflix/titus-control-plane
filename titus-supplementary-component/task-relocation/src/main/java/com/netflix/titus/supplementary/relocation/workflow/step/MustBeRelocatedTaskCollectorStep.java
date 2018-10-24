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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.hasDisruptionBudget;
import static com.netflix.titus.supplementary.relocation.model.RelocationFunctions.areEqualExceptRelocationTime;
import static com.netflix.titus.supplementary.relocation.util.RelocationUtil.getAgentInstances;
import static com.netflix.titus.supplementary.relocation.util.RelocationUtil.getRemovableGroups;

/**
 * Step at which all containers that are requested to terminate are identified, and their relocation timestamps are set.
 */
public class MustBeRelocatedTaskCollectorStep {

    private static final Logger logger = LoggerFactory.getLogger(MustBeRelocatedTaskCollectorStep.class);

    private final ReadOnlyAgentOperations agentOperations;
    private final ReadOnlyJobOperations jobOperations;
    private final StepMetrics metrics;
    private final Clock clock;

    private Map<String, TaskRelocationPlan> lastResult = Collections.emptyMap();

    public MustBeRelocatedTaskCollectorStep(ReadOnlyAgentOperations agentOperations,
                                            ReadOnlyJobOperations jobOperations,
                                            TitusRuntime titusRuntime) {
        this.agentOperations = agentOperations;
        this.jobOperations = jobOperations;
        this.clock = titusRuntime.getClock();
        this.metrics = new StepMetrics("mustBeRelocatedTaskCollectorStep", titusRuntime);
    }

    public Map<String, TaskRelocationPlan> collectTasksThatMustBeRelocated() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Map<String, TaskRelocationPlan> result = execute();
            metrics.onSuccess(result.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            logger.debug("Step results: {}", result);
            return result;
        } catch (Exception e) {
            logger.error("Step processing error", e);
            metrics.onError(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private Map<String, TaskRelocationPlan> execute() {
        List<Task> tasks = jobOperations.getTasks();

        List<AgentInstance> removableAgents = getAgentInstances(agentOperations, getRemovableGroups(agentOperations));

        List<Pair<AgentInstance, Task>> tasksOnRemovableAgents = removableAgents.stream()
                .flatMap(agent -> RelocationUtil.findTasksOnInstance(agent, tasks).stream().map(t -> Pair.of(agent, t)))
                .collect(Collectors.toList());

        long now = clock.wallTime();

        Map<String, TaskRelocationPlan> result = new HashMap<>();
        tasksOnRemovableAgents.forEach(agentTaskPair -> {
            AgentInstance agent = agentTaskPair.getLeft();
            Task task = agentTaskPair.getRight();
            Optional<Job<?>> jobOpt = jobOperations.getJob(task.getJobId());

            if (jobOpt == null) {
                logger.info("Found task with no job record. Ignoring it: jobId={}, taskId={}", task.getJobId(), task.getId());
                return;
            }

            Job<?> job = jobOpt.get();

            if (hasDisruptionBudget(job) && isSelfManaged(job)) {
                SelfManagedDisruptionBudgetPolicy selfManaged = (SelfManagedDisruptionBudgetPolicy) job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();

                TaskRelocationPlan relocationPlan = TaskRelocationPlan.newBuilder()
                        .withTaskId(task.getId())
                        .withReason(TaskRelocationReason.TaskMigration)
                        .withReasonMessage("Agent instance scheduled to remove: instanceId=" + agent.getId())
                        .withRelocationTime(now + selfManaged.getRelocationTimeMs())
                        .build();

                TaskRelocationPlan previous = lastResult.get(task.getId());
                boolean keepPrevious = previous != null &&
                        (areEqualExceptRelocationTime(previous, relocationPlan) || previous.getRelocationTime() < relocationPlan.getRelocationTime());

                result.put(task.getId(), keepPrevious ? previous : relocationPlan);
            }
        });

        this.lastResult = result;

        return result;
    }

    private boolean isSelfManaged(Job<?> job) {
        DisruptionBudgetPolicy disruptionBudgetPolicy = job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        return disruptionBudgetPolicy instanceof SelfManagedDisruptionBudgetPolicy;
    }
}
