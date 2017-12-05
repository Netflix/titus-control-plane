/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;

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

    private static final String TIMEOUT_KILL_INITIATED_TAG = "timeout.killInitiated";
    private static final String KILL_INITIATED_ATTEMPT_TAG = "timeout.killInitiatedAttempt";

    private static final Map<TaskState, String> STATE_TAGS = ImmutableMap.of(
            TaskState.Launched, "timeout.launched",
            TaskState.StartInitiated, "timeout.startInitiated",
            TaskState.KillInitiated, TIMEOUT_KILL_INITIATED_TAG
    );


    public static TitusChangeAction setTimeout(String taskId, TaskState taskState, long deadlineMs) {
        String tagName = STATE_TAGS.get(taskState);
        Preconditions.checkArgument(tagName != null, "Timeout not tracked for state %s", taskState);

        return TitusChangeAction.newAction("setTimeout")
                .id(taskId)
                .trigger(Trigger.Reconciler)
                .summary("Setting timeout for task in state: %s (deadline %s)", taskState, DateTimeExt.toUtcDateTimeString(deadlineMs))
                .applyModelUpdate(self -> {
                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self)
                            .taskMaybeUpdate(jobHolder ->
                                    jobHolder.findById(taskId).map(taskHolder -> {
                                        EntityHolder newTaskHolder = taskHolder.addTag(tagName, deadlineMs);
                                        if (taskState == TaskState.KillInitiated) {
                                            newTaskHolder = newTaskHolder.addTag(KILL_INITIATED_ATTEMPT_TAG, 0);
                                        }
                                        return Pair.of(jobHolder.addChild(newTaskHolder), newTaskHolder);
                                    }));
                    return ModelActionHolder.running(modelAction);
                });
    }

    public static TitusChangeAction incrementTaskKillAttempt(String taskId, long deadlineMs) {
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
                                                .addTag(TIMEOUT_KILL_INITIATED_TAG, deadlineMs)
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
