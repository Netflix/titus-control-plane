/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.relocation.model.event;

import java.util.Objects;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;

public class TaskRelocationPlanUpdateEvent extends TaskRelocationEvent {

    private final TaskRelocationPlan plan;

    public TaskRelocationPlanUpdateEvent(TaskRelocationPlan plan) {
        this.plan = plan;
    }

    public TaskRelocationPlan getPlan() {
        return plan;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskRelocationPlanUpdateEvent that = (TaskRelocationPlanUpdateEvent) o;
        return Objects.equals(plan, that.plan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan);
    }

    @Override
    public String toString() {
        return "TaskRelocationPlanUpdateEvent{" +
                "plan=" + plan +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private TaskRelocationPlan plan;

        private Builder() {
        }

        public Builder withPlan(TaskRelocationPlan plan) {
            this.plan = plan;
            return this;
        }

        public Builder but() {
            return newBuilder().withPlan(plan);
        }

        public TaskRelocationPlanUpdateEvent build() {
            return new TaskRelocationPlanUpdateEvent(plan);
        }
    }
}
