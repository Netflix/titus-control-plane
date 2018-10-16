package com.netflix.titus.common.framework.scheduler;

import java.util.Optional;

import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;

public class ExecutionContext {

    private final String id;
    private final long cycle;
    private final ScheduledAction currentAction;
    private final Optional<ScheduledAction> previousAction;

    private ExecutionContext(String id, long cycle, ScheduledAction currentAction, Optional<ScheduledAction> previousAction) {
        this.id = id;
        this.cycle = cycle;
        this.currentAction = currentAction;
        this.previousAction = previousAction;
    }

    public String getId() {
        return id;
    }

    public long getCycle() {
        return cycle;
    }

    public ScheduledAction getCurrentAction() {
        return currentAction;
    }

    public Optional<ScheduledAction> getPreviousAction() {
        return previousAction;
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withCycle(cycle);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private long cycle;
        private ScheduledAction currentAction;
        private ScheduledAction previousAction;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withCycle(long cycle) {
            this.cycle = cycle;
            return this;
        }

        public Builder withCurrentAction(ScheduledAction currentAction) {
            this.currentAction = currentAction;
            return this;
        }

        public Builder withPreviousAction(ScheduledAction previousAction) {
            this.previousAction = previousAction;
            return this;
        }

        public ExecutionContext build() {
            return new ExecutionContext(id, cycle, currentAction, Optional.ofNullable(previousAction));
        }
    }
}
