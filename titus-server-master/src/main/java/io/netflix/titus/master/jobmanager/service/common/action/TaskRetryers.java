package io.netflix.titus.master.jobmanager.service.common.action;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.time.Clock;

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

    public static Retryer getNextTaskRetryer(Job<?> job, EntityHolder taskHolder) {
        return getCurrentTaskRetryer(taskHolder)
                .map(Retryer::retry)
                .orElseGet(() -> JobFunctions.retryer(job));
    }

    public static long getCurrentRetryerDelayMs(EntityHolder taskHolder, long taskRetryerResetTimeMs, Clock clock) {
        return getCurrentTaskRetryer(taskHolder).map(retryer -> {
            long timeInStartedState = JobFunctions.getTimeInState(taskHolder.getEntity(), TaskState.Started, clock).orElse(0L);
            return timeInStartedState >= taskRetryerResetTimeMs ? 0L : retryer.getDelayMs().orElse(0L);
        }).orElse(0L);
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
