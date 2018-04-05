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

package com.netflix.titus.master.jobmanager.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;

/**
 * {@link TaskStatus} consists of the a task state, and reason code. To report task state metrics, we have to
 * collapse task state/reason code to a single value.
 */
public enum TaskStateReport {
    Accepted,

    Launched,

    StartInitiated,

    Started,

    KillInitiated,

    Disconnected,

    Finished,

    Failed;

    private static final Set<TaskStateReport> SET_OF_ALL = new HashSet<>(Arrays.asList(TaskStateReport.values()));

    public static TaskStateReport of(TaskStatus taskStatus) {
        TaskState taskState = taskStatus.getState();
        switch (taskState) {
            case Accepted:
                return Accepted;
            case Launched:
                return Launched;
            case StartInitiated:
                return StartInitiated;
            case Started:
                return Started;
            case KillInitiated:
                return KillInitiated;
            case Disconnected:
                return Disconnected;
            case Finished:
                return TaskStatus.REASON_NORMAL.equals(taskStatus.getReasonCode()) ? Finished : Failed;
        }
        return Failed;
    }

    public static Set<TaskStateReport> setOfAll() {
        return SET_OF_ALL;
    }

    public static boolean isTerminalState(TaskStateReport state) {
        return state == Finished || state == Failed;
    }
}
