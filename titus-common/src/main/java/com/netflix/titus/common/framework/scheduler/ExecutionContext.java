package com.netflix.titus.common.framework.scheduler;

import java.util.Optional;

import com.netflix.titus.common.framework.scheduler.model.Iteration;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;

public class ExecutionContext {

    private final String id;
    private final Iteration iteration;
    private final ScheduledAction currentAction;
    private final Optional<ScheduledAction> previousAction;

    private ExecutionContext(String id, Iteration iteration, ScheduledAction currentAction, Optional<ScheduledAction> previousAction) {
        this.id = id;
        this.iteration = iteration;
        this.currentAction = currentAction;
        this.previousAction = previousAction;
    }

    public String getId() {
        return id;
    }

    public Iteration getIteration() {
        return iteration;
    }

    public ScheduledAction getCurrentAction() {
        return currentAction;
    }

    public Optional<ScheduledAction> getPreviousAction() {
        return previousAction;
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withIteration(iteration);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private ScheduledAction currentAction;
        private ScheduledAction previousAction;
        private Iteration iteration;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withIteration(Iteration iteration) {
            this.iteration = iteration;
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
            return new ExecutionContext(id, iteration, currentAction, Optional.ofNullable(previousAction));
        }
    }
}
