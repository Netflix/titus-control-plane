/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.util;

import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.RelocationAttributes;
import com.netflix.titus.supplementary.relocation.connector.Node;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.hasDisruptionBudget;

public class RelocationPredicates {

    public enum RelocationTrigger {
        Instance,
        InstanceGroup,
        Job,
        Task
    }

    public static Optional<String> checkIfNeedsRelocationPlan(Job<?> job, Task task, Node instance) {
        if (!hasDisruptionBudget(job) || !isSelfManaged(job)) {
            return Optional.empty();
        }

        if (isRelocationNotAllowed(job) || isRelocationNotAllowed(task) || instance.isRelocationNotAllowed()) {
            return Optional.empty();
        }

        // As the relocation must be done immediately, there is no point in persisting the plan. The task will be
        // evicted in this iteration.
        if (instance.isRelocationRequiredImmediately() || isRelocationRequiredImmediately(task) || isRelocationRequiredByImmediately(job, task)) {
            return Optional.empty();
        }

        if (isRelocationRequired(task)) {
            return Optional.of("Task tagged for relocation");
        }

        if (isRelocationRequiredBy(job, task)) {
            long jobTimestamp = getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY);
            long taskTimestamp = getTaskCreateTimestamp(task);
            if (jobTimestamp >= taskTimestamp) {
                return Optional.of("Job tagged for relocation for tasks created before " + DateTimeExt.toUtcDateTimeString(jobTimestamp));
            }
        }

        if (instance.isRelocationRequired()) {
            return Optional.of("Agent instance tagged for eviction");
        }

        if (instance.isServerGroupRelocationRequired()) {
            return Optional.of("Agent instance group tagged for eviction");
        }

        return Optional.empty();
    }

    public static Optional<Pair<RelocationTrigger, String>> checkIfMustBeRelocatedImmediately(Job<?> job, Task task, Node instance) {
        if (instance.isRelocationRequiredImmediately()) {
            return Optional.of(Pair.of(RelocationTrigger.Instance, "Agent instance tagged for immediate eviction"));
        }

        if (isRelocationRequiredImmediately(task)) {
            return Optional.of(Pair.of(RelocationTrigger.Task, "Task marked for immediate eviction"));
        }

        if (isRelocationRequiredByImmediately(job, task)) {
            return Optional.of(Pair.of(RelocationTrigger.Job, "Job marked for immediate eviction"));
        }

        return Optional.empty();
    }

    public static Optional<Pair<RelocationTrigger, String>> checkIfRelocationRequired(Job<?> job, Task task) {
        if (isRelocationRequired(task)) {
            return Optional.of(Pair.of(RelocationTrigger.Task, "Task marked for eviction"));
        }

        if (isRelocationRequiredBy(job, task)) {
            long timestamp = getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY);
            return Optional.of(Pair.of(RelocationTrigger.Job, String.format("Job tasks created before %s marked for eviction", DateTimeExt.toUtcDateTimeString(timestamp))));
        }

        return Optional.empty();
    }

    public static Optional<Pair<RelocationTrigger, String>> checkIfRelocationRequired(Job<?> job, Task task, Node instance) {
        if (instance.isRelocationRequired()) {
            return Optional.of(Pair.of(RelocationTrigger.Instance, "Agent tagged for eviction"));
        }
        return checkIfRelocationRequired(job, task);
    }

    public static Optional<String> checkIfRelocationBlocked(Job<?> job, Task task, Node instance) {
        if (isRelocationNotAllowed(task)) {
            return Optional.of("Task marked as not evictable");
        }
        if (isRelocationNotAllowed(job)) {
            return Optional.of("Job marked as not evictable");
        }
        if (instance.isRelocationNotAllowed()) {
            return Optional.of("Agent marked as not evictable");
        }
        return Optional.empty();
    }

    private static boolean isRelocationRequired(Task task) {
        return checkRelocationAttribute(task.getAttributes());
    }

    private static boolean isRelocationRequiredImmediately(Task task) {
        return task.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationRequiredByImmediately(Job<?> job, Task task) {
        return getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY) >= getTaskCreateTimestamp(task);
    }

    private static boolean isRelocationRequiredBy(Job<?> job, Task task) {
        return getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY) >= getTaskCreateTimestamp(task);
    }

    private static boolean isRelocationNotAllowed(Job<?> job) {
        return job.getJobDescriptor().getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_NOT_ALLOWED, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationNotAllowed(Task task) {
        return task.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_NOT_ALLOWED, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean checkRelocationAttribute(Map<String, String> attributes) {
        return attributes.getOrDefault(RelocationAttributes.RELOCATION_REQUIRED, "false").equalsIgnoreCase("true");
    }

    private static long getJobTimestamp(Job<?> job, String key) {
        try {
            return Long.parseLong(job.getJobDescriptor().getAttributes().getOrDefault(key, "-1"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static long getTaskCreateTimestamp(Task task) {
        return JobFunctions.findTaskStatus(task, TaskState.Accepted).orElse(task.getStatus()).getTimestamp();
    }

    private static boolean isSelfManaged(Job<?> job) {
        DisruptionBudgetPolicy disruptionBudgetPolicy = job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        return disruptionBudgetPolicy instanceof SelfManagedDisruptionBudgetPolicy;
    }
}
