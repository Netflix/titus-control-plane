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

package com.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;

/**
 * Associates a timeout with transient task states (Launched|StartInitiated). If task does not move out of this state
 * within an expected amount of time, it is killed.
 * <p>
 * <h1>Why cannot use task state timestamp?</h1>
 * Due to TitusMaster failover process, and delayed Mesos state reconciliation. If we would do this, we might
 * kill tasks that progressed their state, but we are not aware of this, due to a delay in the reconciliation process.
 * This could be solved by doing Mesos reconciliation during system bootstrap, before job reconciliation process starts
 * (a possible improvement in the future).
 */
public class TaskTimeoutChangeActions {

    public enum TimeoutStatus {Ignore, NotSet, Pending, TimedOut}

    private static final String KILL_INITIATED_ATTEMPT_TAG = "timeout.killInitiatedAttempt";

    private static final String LAUNCHED_STATE_TIMEOUT_TAG = "timeout.launched";
    private static final String START_INITIATED_TIMEOUT_TAG = "timeout.startInitiated";
    private static final String KILL_INITIATED_TIMEOUT_TAG = "timeout.killInitiated";

    private static final Map<TaskState, String> STATE_TAGS = ImmutableMap.of(
            TaskState.Launched, LAUNCHED_STATE_TIMEOUT_TAG,
            TaskState.StartInitiated, START_INITIATED_TIMEOUT_TAG,
            TaskState.KillInitiated, KILL_INITIATED_TIMEOUT_TAG
    );

    public static TitusChangeAction setTimeout(String taskId, TaskState taskState, long timeoutMs, Clock clock) {
        String tagName = STATE_TAGS.get(taskState);
        Preconditions.checkArgument(tagName != null, "Timeout not tracked for state %s", taskState);

        return TitusChangeAction.newAction("setTimeout")
                .id(taskId)
                .trigger(Trigger.Reconciler)
                .summary("Setting timeout for task in state %s: %s", taskState, DateTimeExt.toTimeUnitString(timeoutMs))
                .applyModelUpdate(self -> {
                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self)
                            .taskMaybeUpdate(jobHolder ->
                                    jobHolder.findById(taskId).map(taskHolder -> {
                                        EntityHolder newTaskHolder = taskHolder.addTag(tagName, clock.wallTime() + timeoutMs);
                                        if (taskState == TaskState.KillInitiated) {
                                            newTaskHolder = newTaskHolder.addTag(KILL_INITIATED_ATTEMPT_TAG, 0);
                                        }
                                        return Pair.of(jobHolder.addChild(newTaskHolder), newTaskHolder);
                                    }));
                    return ModelActionHolder.running(modelAction);
                });
    }

    public static EntityHolder setTimeoutOnRestoreFromStore(JobManagerConfiguration configuration, EntityHolder taskHolder, Clock clock) {
        Task task = taskHolder.getEntity();
        switch (task.getStatus().getState()) {
            case Launched:
                return taskHolder.addTag(LAUNCHED_STATE_TIMEOUT_TAG, clock.wallTime() + configuration.getTaskInLaunchedStateTimeoutMs());
            case StartInitiated:
                long timeoutMs = JobFunctions.isServiceTask(task)
                        ? configuration.getServiceTaskInStartInitiatedStateTimeoutMs()
                        : configuration.getBatchTaskInStartInitiatedStateTimeoutMs();
                return taskHolder.addTag(START_INITIATED_TIMEOUT_TAG, clock.wallTime() + timeoutMs);
            case KillInitiated:
                return taskHolder
                        .addTag(KILL_INITIATED_TIMEOUT_TAG, clock.wallTime() + configuration.getTaskInKillInitiatedStateTimeoutMs())
                        .addTag(KILL_INITIATED_ATTEMPT_TAG, 0);
        }
        return taskHolder;
    }

    public static TitusChangeAction incrementTaskKillAttempt(String taskId, long deadlineMs, Clock clock) {
        return TitusChangeAction.newAction("anotherKillAttempt")
                .id(taskId)
                .trigger(Trigger.Reconciler)
                .summary("Registering another task kill attempt due to timeout in KillInitiated state")
                .applyModelUpdate(self -> {
                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self)
                            .taskMaybeUpdate(jobHolder ->
                                    jobHolder.findById(taskId).map(taskHolder -> {
                                        int attempt = (int) taskHolder.getAttributes().getOrDefault(KILL_INITIATED_ATTEMPT_TAG, 0);
                                        EntityHolder newTaskHolder = taskHolder
                                                .addTag(KILL_INITIATED_TIMEOUT_TAG, clock.wallTime() + deadlineMs)
                                                .addTag(KILL_INITIATED_ATTEMPT_TAG, attempt + 1);
                                        return Pair.of(jobHolder.addChild(newTaskHolder), newTaskHolder);
                                    }));
                    return ModelActionHolder.running(modelAction);
                });
    }

    public static TimeoutStatus getTimeoutStatus(EntityHolder taskHolder, Clock clock) {
        Task task = taskHolder.getEntity();
        TaskState state = task.getStatus().getState();
        if (state != TaskState.Launched && state != TaskState.StartInitiated && state != TaskState.KillInitiated) {
            return TimeoutStatus.Ignore;
        }

        Long deadline = (Long) taskHolder.getAttributes().get(STATE_TAGS.get(state));
        if (deadline == null) {
            return TimeoutStatus.NotSet;
        }
        return clock.wallTime() < deadline ? TimeoutStatus.Pending : TimeoutStatus.TimedOut;
    }

    public static int getKillInitiatedAttempts(EntityHolder taskHolder) {
        return (int) taskHolder.getAttributes().getOrDefault(KILL_INITIATED_ATTEMPT_TAG, 0);
    }
}
