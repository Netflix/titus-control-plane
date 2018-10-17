package com.netflix.titus.common.framework.scheduler;

import java.util.Optional;

import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.TransactionId;

public class ExecutionContext {

    private final String id;
    private final TransactionId transactionId;
    private final ScheduledAction currentAction;
    private final Optional<ScheduledAction> previousAction;

    private ExecutionContext(String id, TransactionId transactionId, ScheduledAction currentAction, Optional<ScheduledAction> previousAction) {
        this.id = id;
        this.transactionId = transactionId;
        this.currentAction = currentAction;
        this.previousAction = previousAction;
    }

    public String getId() {
        return id;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public ScheduledAction getCurrentAction() {
        return currentAction;
    }

    public Optional<ScheduledAction> getPreviousAction() {
        return previousAction;
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withTransactionId(transactionId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private ScheduledAction currentAction;
        private ScheduledAction previousAction;
        private TransactionId transactionId;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withTransactionId(TransactionId transactionId) {
            this.transactionId = transactionId;
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
            return new ExecutionContext(id, transactionId, currentAction, Optional.ofNullable(previousAction));
        }
    }
}
