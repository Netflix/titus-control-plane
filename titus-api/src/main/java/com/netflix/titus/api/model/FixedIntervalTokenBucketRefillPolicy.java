package com.netflix.titus.api.model;

import java.util.Objects;

public class FixedIntervalTokenBucketRefillPolicy extends TokenBucketRefillPolicy {

    private final long numberOfTokensPerInterval;
    private final long intervalMs;

    public FixedIntervalTokenBucketRefillPolicy(long numberOfTokensPerInterval, long intervalMs) {
        this.numberOfTokensPerInterval = numberOfTokensPerInterval;
        this.intervalMs = intervalMs;
    }

    public long getNumberOfTokensPerInterval() {
        return numberOfTokensPerInterval;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FixedIntervalTokenBucketRefillPolicy that = (FixedIntervalTokenBucketRefillPolicy) o;
        return numberOfTokensPerInterval == that.numberOfTokensPerInterval &&
                intervalMs == that.intervalMs;
    }

    @Override
    public int hashCode() {

        return Objects.hash(numberOfTokensPerInterval, intervalMs);
    }

    @Override
    public String toString() {
        return "FixedIntervalTokenBucketRefillPolicy{" +
                "numberOfTokensPerInterval=" + numberOfTokensPerInterval +
                ", intervalMs=" + intervalMs +
                "} " + super.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private long numberOfTokensPerInterval;
        private long intervalMs;

        private Builder() {
        }

        public Builder withNumberOfTokensPerInterval(long numberOfTokensPerInterval) {
            this.numberOfTokensPerInterval = numberOfTokensPerInterval;
            return this;
        }

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public FixedIntervalTokenBucketRefillPolicy build() {
            return new FixedIntervalTokenBucketRefillPolicy(numberOfTokensPerInterval, intervalMs);
        }
    }
}
