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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO Add 'Scheduled' state in between 'Accepted' and 'Launched' states to denote Fenzo task placement decision.
 */
public enum TaskState {
    /**
     * A task was passed to scheduler, but has no resources allocated yet.
     */
    Accepted,

    /**
     * A task had resources allocated, and was passed to Mesos
     */
    Launched,

    /**
     * An executor provisioned resources for a task.
     */
    StartInitiated,

    /**
     * Task's container started.
     */
    Started,

    /**
     * A user requested the task to be terminated. An executor is stopping the task, and releasing its allocated resources.
     */
    KillInitiated,

    /**
     * No connectivity between Mesos and an agent running a task. Task's state cannot be determined until the connection is established again.
     */
    Disconnected,

    /**
     * A task finished or was forced by a user to terminate. All resources previously assigned to this task are released.
     */
    Finished;

    private static final Set<TaskState> SET_OF_ALL = new HashSet<>(Arrays.asList(TaskState.values()));

    public static boolean isRunning(TaskState taskState) {
        switch (taskState) {
            case Launched:
            case StartInitiated:
            case Started:
            case KillInitiated:
            case Disconnected:
                return true;
            case Accepted:
            case Finished:
            default:
                return false;
        }
    }

    public static boolean isTerminalState(TaskState taskState) {
        return taskState == TaskState.Finished;
    }

    public static boolean isBefore(TaskState checked, TaskState reference) {
        if (checked == TaskState.Disconnected || reference == TaskState.Disconnected) {
            return false;
        }
        return checked.ordinal() < reference.ordinal();
    }

    public static boolean isAfter(TaskState checked, TaskState reference) {
        if (checked == TaskState.Disconnected || reference == TaskState.Disconnected) {
            return false;
        }
        return checked.ordinal() > reference.ordinal();
    }

    public static Set<TaskState> setOfAll() {
        return SET_OF_ALL;
    }
}
