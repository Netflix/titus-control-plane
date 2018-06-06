package com.netflix.titus.common.runtime;

import java.util.Objects;

/**
 * System abort event.
 */
public class SystemAbortEvent {

    public enum FailureType {
        /**
         * There is an error requiring system restart, but the system is expected to recover without human intervention.
         */
        Recoverable,

        /**
         * There is a non-recoverable system error, which requires direct involvement of the system administrator.
         */
        Nonrecoverable
    }

    private final String failureId;
    private final FailureType failureType;
    private final String reason;
    private final long timestamp;

    public SystemAbortEvent(String failureId,
                            FailureType failureType,
                            String reason,
                            long timestamp) {
        this.failureId = failureId;
        this.failureType = failureType;
        this.reason = reason;
        this.timestamp = timestamp;
    }

    public String getFailureId() {
        return failureId;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public String getReason() {
        return reason;
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
        SystemAbortEvent that = (SystemAbortEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(failureId, that.failureId) &&
                failureType == that.failureType &&
                Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failureId, failureType, reason, timestamp);
    }

    @Override
    public String toString() {
        return "SystemAbortEvent{" +
                "failureId='" + failureId + '\'' +
                ", failureType=" + failureType +
                ", reason='" + reason + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String failureId;
        private FailureType failureType;
        private String reason;
        private long timestamp;

        private Builder() {
        }

        public Builder withFailureId(String failureId) {
            this.failureId = failureId;
            return this;
        }

        public Builder withFailureType(FailureType failureType) {
            this.failureType = failureType;
            return this;
        }

        public Builder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withFailureId(failureId).withFailureType(failureType).withReason(reason).withTimestamp(timestamp);
        }

        public SystemAbortEvent build() {
            return new SystemAbortEvent(failureId, failureType, reason, timestamp);
        }
    }
}
