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

package com.netflix.titus.master.jobmanager.service.common.action;

import java.util.Optional;
import java.util.function.Supplier;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;

/**
 * A set of primitive functions for dealing with task retry rules. Each task is associated at its creation time
 * with a {@link Retryer} instance. This instance determines the retry delay time in case the current task fails.
 * The delay is measured from the time when the task moved to Finished state.
 * The resubmit delay time is computed and added to the task reference model, at the very same moment as the task
 * state change is recorded in it. Resubmit delay is also added to task context, as a hint to external clients why
 * the resubmit process is delayed.
 */
public class TaskRetryers {

    public static final String ATTR_TASK_RETRY = "retryer";
    public static final String ATTR_TASK_RETRY_DELAY_MS = "retryDelayMs";

    public static Optional<Retryer> getCurrentTaskRetryer(EntityHolder taskHolder) {
        return Optional.ofNullable((Retryer) taskHolder.getAttributes().get(ATTR_TASK_RETRY));
    }

    public static Retryer getNextTaskRetryer(Supplier<Retryer> systemRetryer, Job<?> job, EntityHolder taskHolder) {
        return getCurrentTaskRetryer(taskHolder)
                .map(Retryer::retry)
                .orElseGet(() -> Retryers.max(systemRetryer.get(), JobFunctions.retryer(job)));
    }

    public static long getCurrentRetryerDelayMs(EntityHolder taskHolder, long minRetryIntervalMs, long taskRetryerResetTimeMs, Clock clock) {
        return getCurrentTaskRetryer(taskHolder).map(retryer -> {
            long timeInStartedState = JobFunctions.getTimeInState(taskHolder.getEntity(), TaskState.Started, clock).orElse(0L);
            return timeInStartedState >= taskRetryerResetTimeMs ? 0L : Math.max(minRetryIntervalMs, retryer.getDelayMs().orElse(minRetryIntervalMs));
        }).orElse(minRetryIntervalMs);
    }

    public static boolean shouldRetryNow(EntityHolder taskHolder, Clock clock) {
        long delayMs = (long) taskHolder.getAttributes().getOrDefault(ATTR_TASK_RETRY_DELAY_MS, 0L);
        if (delayMs == 0) {
            return true;
        }
        Task task = taskHolder.getEntity();
        long delayUntil = task.getStatus().getTimestamp() + delayMs;
        return delayUntil <= clock.wallTime();
    }
}
