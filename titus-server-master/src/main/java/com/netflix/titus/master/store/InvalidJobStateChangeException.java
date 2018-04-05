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

package com.netflix.titus.master.store;

import com.netflix.titus.api.model.v2.V2JobState;

public class InvalidJobStateChangeException extends Exception {
    public InvalidJobStateChangeException(String jobId, V2JobState state) {
        super("Unexpected state " + state + " for job " + jobId);
    }

    public InvalidJobStateChangeException(String jobId, V2JobState state, Throwable t) {
        super("Unexpected state " + state + " for job " + jobId, t);
    }

    public InvalidJobStateChangeException(String jobId, V2JobState fromState, V2JobState toState) {
        super("Invalid state transition of job " + jobId + " from state " + fromState + " to " + toState);
    }

    public InvalidJobStateChangeException(String jobId, V2JobState fromState, V2JobState toState, Throwable cause) {
        super("Invalid state transition of job " + jobId + " from state " + fromState + " to " + toState, cause);
    }
}
