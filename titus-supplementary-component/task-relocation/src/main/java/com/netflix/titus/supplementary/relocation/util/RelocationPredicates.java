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

import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.supplementary.relocation.RelocationAttributes;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.hasDisruptionBudget;

public class RelocationPredicates {

    public static Optional<String> checkIfNeedsRelocationPlan(Job<?> job,
                                                              Task task,
                                                              AgentInstanceGroup instanceGroup,
                                                              AgentInstance instance) {
        if (!hasDisruptionBudget(job) || !isSelfManaged(job)) {
            return Optional.empty();
        }

        if (isRelocationNotAllowed(job) || isRelocationNotAllowed(task) || isRelocationNotAllowed(instance)) {
            return Optional.empty();
        }

        // As the relocation must be done immediately, there is no point in persisting the plan. The task will be
        // evicted in this iteration.
        if (isRelocationRequiredImmediately(instance) || isRelocationRequired(task) || isRelocationRequiredByImmediately(job, task)) {
            return Optional.empty();
        }

        if (isRelocationRequired(task)) {
            return Optional.of("Task tagged for relocation");
        }

        if (isRelocationRequiredBy(job)) {
            long jobTimestamp = getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY);
            long taskTimestamp = getTaskCreateTimestamp(task);
            if (jobTimestamp >= taskTimestamp) {
                return Optional.of("Job tagged for relocation for tasks created before " + DateTimeExt.toTimeUnitString(jobTimestamp));
            }
        }

        if (isRelocationRequired(instance)) {
            return Optional.of("Agent instance tagged for eviction");
        }

        if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable) {
            return Optional.of("Agent instance group tagged for eviction");
        }

        return Optional.empty();
    }

    public static Optional<String> checkIfMustBeRelocatedImmediately(Job<?> job, Task task, AgentInstance instance) {
        if (isRelocationRequiredImmediately(instance)) {
            return Optional.of("Agent instance tagged for immediate eviction");
        }

        if (isRelocationRequiredImmediately(task)) {
            return Optional.of("Task marked for immediate eviction");
        }

        if (isRelocationRequiredByImmediately(job, task)) {
            return Optional.of("Job marked for immediate eviction");
        }

        return Optional.empty();
    }

    public static Optional<String> checkIfRelocationBlocked(Job<?> job, Task task, AgentInstance instance) {
        if (isRelocationNotAllowed(task)) {
            return Optional.of("Task marked as not evictable");
        }
        if (isRelocationNotAllowed(job)) {
            return Optional.of("Job marked as not evictable");
        }
        if (isRelocationNotAllowed(instance)) {
            return Optional.of("Agent marked as not evictable");
        }
        return Optional.empty();
    }

    private static boolean isRelocationRequired(AgentInstance agentInstance) {
        return agentInstance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationRequiredImmediately(AgentInstance agentInstance) {
        return agentInstance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationRequired(Task task) {
        return task.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationRequiredImmediately(Task task) {
        return task.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "false")
                .equalsIgnoreCase("true");
    }

    private static boolean isRelocationRequiredByImmediately(Job<?> job, Task task) {
        return getJobTimestamp(job, RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY) >= getTaskCreateTimestamp(task);
    }

    private static boolean isRelocationRequiredBy(Job<?> job) {
        return job.getJobDescriptor().getAttributes().containsKey(RelocationAttributes.RELOCATION_REQUIRED_BY);
    }

    private static boolean isRelocationNotAllowed(AgentInstance agentInstance) {
        return agentInstance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_NOT_ALLOWED, "false")
                .equalsIgnoreCase("true");
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

    private static long getJobTimestamp(Job<?> job, String key) {
        try {
            return Long.parseLong(job.getJobDescriptor().getAttributes().getOrDefault(key, "0"));
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
