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

package com.netflix.titus.master.eviction.service.quota.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;

/**
 * TODO This information should be persisted somehow. We could encode it in the task kill reason code/message.
 */
public class TaskRelocationLimitController implements QuotaController<Job<?>> {

    private final Job<?> job;
    private final V3JobOperations jobOperations;
    private final int perTaskLimit;

    private static final ConsumptionResult TASK_NOT_FOUND = ConsumptionResult.rejected("Task not found");

    private final Map<String, Integer> relocationCountersById;
    private final ConsumptionResult taskLimitExceeded;

    public TaskRelocationLimitController(Job<?> job, V3JobOperations jobOperations) {
        this.job = job;
        this.jobOperations = jobOperations;
        this.perTaskLimit = computePerTaskLimit(job);
        this.relocationCountersById = new HashMap<>();
        this.taskLimitExceeded = buildTaskRelocationLimitExceeded();
    }

    private TaskRelocationLimitController(Job<?> updatedJob,
                                          int perTaskLimit,
                                          TaskRelocationLimitController previous) {
        this.job = updatedJob;
        this.jobOperations = previous.jobOperations;
        this.perTaskLimit = perTaskLimit;
        this.relocationCountersById = previous.relocationCountersById;
        this.taskLimitExceeded = buildTaskRelocationLimitExceeded();
    }

    @Override
    public long getQuota() {
        List<Task> tasks;
        try {
            tasks = jobOperations.getTasks(job.getId());
        } catch (JobManagerException e) {
            return 0;
        }

        int quota = 0;
        for (Task task : tasks) {
            Integer counter = relocationCountersById.get(task.getOriginalId());
            if (counter == null || counter < perTaskLimit) {
                quota++;
            }
        }

        return quota;
    }

    @Override
    public ConsumptionResult consume(String taskId) {
        Optional<Pair<Job<?>, Task>> jobTaskPair = jobOperations.findTaskById(taskId);
        if (!jobTaskPair.isPresent()) {
            return TASK_NOT_FOUND;
        }
        Task task = jobTaskPair.get().getRight();

        int counter = relocationCountersById.getOrDefault(task.getOriginalId(), 0);
        if (counter >= perTaskLimit) {
            return taskLimitExceeded;
        }
        relocationCountersById.put(task.getOriginalId(), counter + 1);
        return ConsumptionResult.approved();
    }

    @Override
    public void giveBackConsumedQuota(String taskId) {
        Optional<Pair<Job<?>, Task>> jobTaskPair = jobOperations.findTaskById(taskId);
        if (!jobTaskPair.isPresent()) {
            return;
        }
        Task task = jobTaskPair.get().getRight();
        Integer counter = relocationCountersById.get(task.getOriginalId());
        if (counter != null && counter > 0) {
            relocationCountersById.put(task.getOriginalId(), counter - 1);
        }
    }

    @Override
    public TaskRelocationLimitController update(Job<?> updatedJob) {
        int perTaskLimit = computePerTaskLimit(updatedJob);
        if (perTaskLimit == this.perTaskLimit) {
            return this;
        }
        return new TaskRelocationLimitController(updatedJob, perTaskLimit, this);
    }

    private static int computePerTaskLimit(Job<?> job) {
        RelocationLimitDisruptionBudgetPolicy policy = (RelocationLimitDisruptionBudgetPolicy)
                job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        return policy.getLimit();
    }

    private ConsumptionResult buildTaskRelocationLimitExceeded() {
        return ConsumptionResult.rejected("Task relocation limit exceeded (limit=" + perTaskLimit + ')');
    }
}
