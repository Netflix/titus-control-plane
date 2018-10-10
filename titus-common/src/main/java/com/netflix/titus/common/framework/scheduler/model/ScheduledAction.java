package com.netflix.titus.common.framework.scheduler.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

public class ScheduledAction {

    private final String id;
    private final SchedulingStatus status;
    private final List<SchedulingStatus> statusHistory;

    public ScheduledAction(String id, SchedulingStatus status, List<SchedulingStatus> statusHistory) {
        this.id = id;
        this.status = status;
        this.statusHistory = statusHistory;
    }

    public String getId() {
        return id;
    }

    public SchedulingStatus getStatus() {
        return status;
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
                Objects.equals(statusHistory, that.statusHistory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, statusHistory);
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withStatus(status).withStatusHistory(statusHistory);
    }

    @Override
    public String toString() {
        return "ScheduledAction{" +
                "id='" + id + '\'' +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private SchedulingStatus status;
        private List<SchedulingStatus> statusHistory = Collections.emptyList();

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

        public ScheduledAction build() {
            Preconditions.checkNotNull(id, "Id cannot be null");
            Preconditions.checkNotNull(status, "Status cannot be null");
            Preconditions.checkNotNull(statusHistory, "Status history cannot be null");

            return new ScheduledAction(id, status, statusHistory);
        }
    }
}
