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

package com.netflix.titus.supplementary.relocation.model;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;

public class DeschedulingResult {

    private final TaskRelocationPlan taskRelocationPlan;
    private final Task task;
    private final AgentInstance agentInstance;
    private final Optional<DeschedulingFailure> failure;

    public DeschedulingResult(TaskRelocationPlan taskRelocationPlan,
                              Task task,
                              AgentInstance agentInstance,
                              Optional<DeschedulingFailure> failure) {
        this.taskRelocationPlan = taskRelocationPlan;
        this.task = task;
        this.agentInstance = agentInstance;
        this.failure = failure;
    }

    public TaskRelocationPlan getTaskRelocationPlan() {
        return taskRelocationPlan;
    }

    public Task getTask() {
        return task;
    }

    public AgentInstance getAgentInstance() {
        return agentInstance;
    }

    public boolean canEvict() {
        return !failure.isPresent();
    }

    public Optional<DeschedulingFailure> getFailure() {
        return failure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeschedulingResult result = (DeschedulingResult) o;
        return Objects.equals(taskRelocationPlan, result.taskRelocationPlan) &&
                Objects.equals(task, result.task) &&
                Objects.equals(agentInstance, result.agentInstance) &&
                Objects.equals(failure, result.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskRelocationPlan, task, agentInstance, failure);
    }

    @Override
    public String toString() {
        return "DeschedulingResult{" +
                "taskRelocationPlan=" + taskRelocationPlan +
                ", task=" + task +
                ", agentInstance=" + agentInstance +
                ", failure=" + failure +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withTaskRelocationPlan(taskRelocationPlan).withTask(task).withAgentInstance(agentInstance).withFailure(failure.orElse(null));
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private TaskRelocationPlan taskRelocationPlan;
        private Task task;
        private AgentInstance agentInstance;
        private Optional<DeschedulingFailure> failure = Optional.empty();

        private Builder() {
        }

        public Builder withTaskRelocationPlan(TaskRelocationPlan taskRelocationPlan) {
            this.taskRelocationPlan = taskRelocationPlan;
            return this;
        }

        public Builder withTask(Task task) {
            this.task = task;
            return this;
        }

        public Builder withAgentInstance(AgentInstance agentInstance) {
            this.agentInstance = agentInstance;
            return this;
        }

        public Builder withFailure(DeschedulingFailure failure) {
            this.failure = Optional.ofNullable(failure);
            return this;
        }

        public DeschedulingResult build() {
            return new DeschedulingResult(taskRelocationPlan, task, agentInstance, failure);
        }
    }
}
