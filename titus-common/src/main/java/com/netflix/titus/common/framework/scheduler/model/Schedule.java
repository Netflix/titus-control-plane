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

package com.netflix.titus.common.framework.scheduler.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

public class Schedule {

    private final String id;
    private final ScheduleDescriptor descriptor;
    private final ScheduledAction currentAction;
    private final List<ScheduledAction> completedActions;

    public Schedule(String id,
                    ScheduleDescriptor descriptor,
                    ScheduledAction currentAction,
                    List<ScheduledAction> completedActions) {
        this.id = id;
        this.descriptor = descriptor;
        this.currentAction = currentAction;
        this.completedActions = completedActions;
    }

    public String getId() {
        return id;
    }

    public ScheduleDescriptor getDescriptor() {
        return descriptor;
    }

    public ScheduledAction getCurrentAction() {
        return currentAction;
    }

    public List<ScheduledAction> getCompletedActions() {
        return completedActions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schedule schedule = (Schedule) o;
        return Objects.equals(id, schedule.id) &&
                Objects.equals(descriptor, schedule.descriptor) &&
                Objects.equals(currentAction, schedule.currentAction) &&
                Objects.equals(completedActions, schedule.completedActions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, descriptor, currentAction, completedActions);
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withDescriptor(descriptor).withCurrentAction(currentAction).withCompletedActions(completedActions);
    }

    @Override
    public String toString() {
        return "Schedule{" +
                "id='" + id + '\'' +
                ", descriptor=" + descriptor +
                ", currentAction=" + currentAction +
                ", completedActions=" + completedActions +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private ScheduleDescriptor descriptor;
        private ScheduledAction currentAction;
        private List<ScheduledAction> completedActions = Collections.emptyList();

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withDescriptor(ScheduleDescriptor descriptor) {
            this.descriptor = descriptor;
            return this;
        }

        public Builder withCurrentAction(ScheduledAction currentAction) {
            this.currentAction = currentAction;
            return this;
        }

        public Builder withCompletedActions(List<ScheduledAction> completedActions) {
            this.completedActions = completedActions;
            return this;
        }

        public Schedule build() {
            Preconditions.checkNotNull(id, "id cannot be null");
            Preconditions.checkNotNull(descriptor, "descriptor cannot be null");
            Preconditions.checkNotNull(currentAction, "currentAction cannot be null");
            Preconditions.checkNotNull(completedActions, "completedActions cannot be null");

            return new Schedule(id, descriptor, currentAction, completedActions);
        }
    }
}
