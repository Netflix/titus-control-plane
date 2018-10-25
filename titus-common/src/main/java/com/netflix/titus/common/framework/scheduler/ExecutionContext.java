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

package com.netflix.titus.common.framework.scheduler;

import java.util.Optional;

import com.netflix.titus.common.framework.scheduler.model.ExecutionId;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;

public class ExecutionContext {

    private final String id;
    private final ExecutionId executionId;
    private final ScheduledAction currentAction;
    private final Optional<ScheduledAction> previousAction;

    private ExecutionContext(String id, ExecutionId executionId, ScheduledAction currentAction, Optional<ScheduledAction> previousAction) {
        this.id = id;
        this.executionId = executionId;
        this.currentAction = currentAction;
        this.previousAction = previousAction;
    }

    public String getId() {
        return id;
    }

    public ExecutionId getExecutionId() {
        return executionId;
    }

    public ScheduledAction getCurrentAction() {
        return currentAction;
    }

    public Optional<ScheduledAction> getPreviousAction() {
        return previousAction;
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withIteration(executionId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private ScheduledAction currentAction;
        private ScheduledAction previousAction;
        private ExecutionId executionId;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withIteration(ExecutionId executionId) {
            this.executionId = executionId;
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
            return new ExecutionContext(id, executionId, currentAction, Optional.ofNullable(previousAction));
        }
    }
}
