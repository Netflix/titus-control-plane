/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.grpc.protogen.NetworkConfiguration;

class ComputeProviderTask {

    private enum State {
        Accepted,
        Scheduled,
        StartInitiated,
        Started,
        Terminating,
        FinishedSucceeded,
        FinishedFailed,
    }

    private State state;

    private final Map<String, String> scheduledTaskContext;

    ComputeProviderTask(Job<?> job, Task task) {
        this.state = State.Accepted;
        this.scheduledTaskContext = CollectionsExt.asMap(
                TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "agent1"
        );
    }

    public State getState() {
        return state;
    }

    public Map<String, String> getScheduledTaskContext() {
        return scheduledTaskContext;
    }

    public boolean isFinished() {
        return state == State.FinishedSucceeded || state == State.FinishedFailed;
    }

    void schedule() {
        Preconditions.checkState(state == State.Accepted);
        this.state = State.Scheduled;
    }

    void terminate() {
        switch (state) {
            case Accepted:
            case Scheduled:
                this.state = State.FinishedFailed;
                break;
            case StartInitiated:
            case Started:
                this.state = State.Terminating;
                break;
            case Terminating:
            case FinishedSucceeded:
            case FinishedFailed:
                // Do not take any actions for these states.
                break;
        }
    }
}
