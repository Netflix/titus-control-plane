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

public class ScheduledAction {

    private final String id;
    private final SchedulingStatus status;
    private final List<SchedulingStatus> statusHistory;
    private final TransactionId transactionId;

    private ScheduledAction(String id, SchedulingStatus status, List<SchedulingStatus> statusHistory, TransactionId transactionId) {
        this.id = id;
        this.status = status;
        this.statusHistory = statusHistory;
        this.transactionId = transactionId;
    }

    public String getId() {
        return id;
    }

    public SchedulingStatus getStatus() {
        return status;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public List<SchedulingStatus> getStatusHistory() {
        return statusHistory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduledAction that = (ScheduledAction) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(status, that.status) &&
                Objects.equals(statusHistory, that.statusHistory) &&
                Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, statusHistory, transactionId);
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withStatus(status).withStatusHistory(statusHistory).withTransactionId(transactionId);
    }

    @Override
    public String toString() {
        return "ScheduledAction{" +
                "id='" + id + '\'' +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                ", transactionId=" + transactionId +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private SchedulingStatus status;
        private List<SchedulingStatus> statusHistory = Collections.emptyList();
        private TransactionId transactionId;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withStatus(SchedulingStatus status) {
            this.status = status;
            return this;
        }

        public Builder withStatusHistory(List<SchedulingStatus> statusHistory) {
            this.statusHistory = statusHistory;
            return this;
        }

        public Builder withTransactionId(TransactionId transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public ScheduledAction build() {
            Preconditions.checkNotNull(id, "Id cannot be null");
            Preconditions.checkNotNull(status, "Status cannot be null");
            Preconditions.checkNotNull(statusHistory, "Status history cannot be null");
            Preconditions.checkNotNull(transactionId, "Transaction id cannot be null");

            return new ScheduledAction(id, status, statusHistory, transactionId);
        }
    }
}
