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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.eviction.service.quota.QuotaController;

/**
 * TODO This information should be persisted somehow. We could encode it in the task kill reason code/message.
 */
public class TaskRelocationLimitController implements QuotaController<Job<?>> {

    private final Job<?> job;
    private final V3JobOperations jobOperations;
    private final int perTaskLimit;

    private final Map<String, Integer> relocationCountersById;

    public TaskRelocationLimitController(Job<?> job, V3JobOperations jobOperations) {
        this.job = job;
        this.jobOperations = jobOperations;
        this.perTaskLimit = computePerTaskLimit(job);
        this.relocationCountersById = new HashMap<>();
    }

    private TaskRelocationLimitController(Job<?> updatedJob,
                                          int perTaskLimit,
                                          TaskRelocationLimitController previous) {
        this.job = updatedJob;
        this.jobOperations = previous.jobOperations;
        this.perTaskLimit = perTaskLimit;
        this.relocationCountersById = previous.relocationCountersById;
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
        Set<String> taskIds = new HashSet<>();
        for (Task task : tasks) {
            taskIds.add(task.getId());
            Integer counter = relocationCountersById.get(task.getId());
            if (counter == null || counter < perTaskLimit) {
                quota++;
            }
        }
        relocationCountersById.keySet().removeIf(tid -> !taskIds.contains(tid));

        return quota;
    }

    @Override
    public boolean consume(String taskId) {
        int counter = relocationCountersById.getOrDefault(taskId, 0);
        if (counter >= perTaskLimit) {
            return false;
        }
        relocationCountersById.put(taskId, counter + 1);
        return true;
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
}
