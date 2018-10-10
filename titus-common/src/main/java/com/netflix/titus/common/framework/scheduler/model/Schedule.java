package com.netflix.titus.common.framework.scheduler.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

public class Schedule {

    private final String id;
    private final ScheduleDescriptor descriptor;
    private final Optional<ScheduledAction> currentAction;
    private final List<ScheduledAction> completedActions;

    public Schedule(String id,
                    ScheduleDescriptor descriptor,
                    Optional<ScheduledAction> currentAction,
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

    public Optional<ScheduledAction> getCurrentAction() {
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
        private Optional<ScheduledAction> currentAction = Optional.empty();
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

        public Builder withCurrentAction(Optional<ScheduledAction> currentAction) {
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
