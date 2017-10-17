/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job;

import io.netflix.titus.common.util.DateTimeExt;

/**
 */
public abstract class ExecutableStatus<STATE extends Enum<STATE>> {

    private final STATE state;
    private final String reasonCode;
    private final String reasonMessage;
    private final long timestamp;

    protected ExecutableStatus(STATE state, String reasonCode, String reasonMessage, long timestamp) {
        this.state = state;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.timestamp = timestamp;
    }

    public STATE getState() {
        return state;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExecutableStatus<?> that = (ExecutableStatus<?>) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (state != null ? !state.equals(that.state) : that.state != null) {
            return false;
        }
        if (reasonCode != null ? !reasonCode.equals(that.reasonCode) : that.reasonCode != null) {
            return false;
        }
        return reasonMessage != null ? reasonMessage.equals(that.reasonMessage) : that.reasonMessage == null;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (reasonCode != null ? reasonCode.hashCode() : 0);
        result = 31 * result + (reasonMessage != null ? reasonMessage.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ExecutableStatus{" +
                "state=" + state +
                ", reasonCode='" + reasonCode + '\'' +
                ", reasonMessage='" + reasonMessage + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static abstract class AbstractBuilder<
            STATE extends Enum<STATE>,
            B extends AbstractBuilder<STATE, B, S>,
            S extends ExecutableStatus<STATE>
            > {
        protected STATE state;
        protected String reasonCode;
        protected String reasonMessage;
        protected long timestamp = System.currentTimeMillis();

        AbstractBuilder() {
        }

        AbstractBuilder(S status) {
            this.state = status.getState();
            this.reasonCode = status.getReasonCode();
            this.reasonMessage = status.getReasonMessage();
            this.timestamp = status.getTimestamp();
        }

        public B withState(STATE state) {
            this.state = state;
            return self();
        }

        public B withReasonCode(String reasonCode) {
            this.reasonCode = reasonCode;
            return self();
        }

        public B withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return self();
        }

        public B withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        public abstract S build();

        private B self() {
            return (B) this;
        }

        protected String toCompleteReasonMessage() {
            StringBuilder messageBuilder = new StringBuilder("Changed state to ")
                    .append(state).append(" on ").append(DateTimeExt.toUtcDateTimeString(timestamp)).append('.');
            if (reasonMessage != null) {
                messageBuilder.append(' ').append(reasonMessage);
            }
            return messageBuilder.toString();
        }
    }
}
