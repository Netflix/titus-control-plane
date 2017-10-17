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

package io.netflix.titus.api.model.v2;

import java.util.HashMap;
import java.util.Map;

import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;

public enum V2JobState {
    Accepted,
    Launched, // scheduled and sent to slave
    StartInitiated, // initial message from slave worker, about to start
    Started, // actually started running
    Failed,  // OK to handle as a resubmit
    Completed, // terminal state, not necessarily successful
    Noop; // internal use only

    private static final Map<V2JobState, V2JobState[]> validChanges;
    private static final Map<V2JobState, MetaState> metaStates;

    static {
        validChanges = new HashMap<>();
        validChanges.put(Accepted, new V2JobState[]{Launched, Failed, Completed});
        validChanges.put(Launched, new V2JobState[]{StartInitiated, Started, Failed, Completed});
        validChanges.put(StartInitiated, new V2JobState[]{StartInitiated, Started, Failed});
        validChanges.put(Started, new V2JobState[]{Started, Completed, Failed});
        validChanges.put(Failed, new V2JobState[]{});
        validChanges.put(Completed, new V2JobState[]{});
        metaStates = new HashMap<>();
        metaStates.put(Accepted, MetaState.Active);
        metaStates.put(Launched, MetaState.Active);
        metaStates.put(StartInitiated, MetaState.Active);
        metaStates.put(Started, MetaState.Active);
        metaStates.put(Failed, MetaState.Terminal);
        metaStates.put(Completed, MetaState.Terminal);
    }

    public enum MetaState {
        Active,
        Terminal
    }

    public static MetaState toMetaState(V2JobState state) {
        return metaStates.get(state);
    }

    public boolean isValidStateChgTo(V2JobState newState) {
        for (V2JobState validState : validChanges.get(this)) {
            if (validState == newState) {
                return true;
            }
        }
        return false;
    }

    public static boolean isTerminalState(V2JobState state) {
        switch (state) {
            case Failed:
            case Completed:
                return true;
            default:
                return false;
        }
    }

    public static boolean isErrorState(V2JobState started) {
        switch (started) {
            case Failed:
                return true;
            default:
                return false;
        }
    }

    public static boolean isRunningState(V2JobState state) {
        switch (state) {
            case Launched:
            case StartInitiated:
            case Started:
                return true;
            default:
                return false;
        }
    }

    public static boolean isStartedState(V2JobState state) {
        return state == Started;
    }

    public static boolean isOnSlaveState(V2JobState state) {
        switch (state) {
            case StartInitiated:
            case Started:
                return true;
            default:
                return false;
        }
    }

    public static TaskState toV3TaskState(V2JobState v2TaskState) {
        switch (v2TaskState) {
            case Accepted:
                return TaskState.Accepted;
            case Launched:
                return TaskState.Launched;
            case StartInitiated:
                return TaskState.StartInitiated;
            case Started:
                return TaskState.Started;
            case Completed:
            case Failed:
        }
        return TaskState.Finished;
    }

    public static String toV3ReasonCode(V2JobState v2TaskState, JobCompletedReason reason) {
        if (v2TaskState != V2JobState.Failed) {
            return TaskStatus.REASON_NORMAL;
        }
        switch (reason) {
            case Normal:
                return TaskStatus.REASON_NORMAL;
            case Error:
                return TaskStatus.REASON_ERROR;
            case Lost:
                return TaskStatus.REASON_TASK_LOST;
            case Killed:
                return TaskStatus.REASON_TASK_KILLED;
            case Failed:
                return TaskStatus.REASON_FAILED;
        }
        return TaskStatus.REASON_UNKNOWN;
    }
}
