package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CallMetadata {
    private final String callerId;
    private final String callReason;
    private final List<String> callPath;
    private final boolean debug;

    public CallMetadata(String callerId, String callReason, List<String> callPath, boolean debug) {
        this.callerId = callerId;
        this.callReason = callReason;
        this.callPath = callPath;
        this.debug = debug;
    }

    public String getCallerId() {
        return callerId;
    }

    public String getCallReason() {
        return callReason;
    }

    public List<String> getCallPath() {
        return callPath;
    }

    public boolean isDebug() {
        return debug;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CallMetadata that = (CallMetadata) o;
        return debug == that.debug &&
                Objects.equals(callerId, that.callerId) &&
                Objects.equals(callReason, that.callReason) &&
                Objects.equals(callPath, that.callPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(callerId, callReason, callPath, debug);
    }

    @Override
    public String toString() {
        return "CallMetadata{" +
                "callerId='" + callerId + '\'' +
                ", callReason='" + callReason + '\'' +
                ", callPath=" + callPath +
                ", debug=" + debug +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return newBuilder().withCallerId(callerId).withCallReason(callReason).withCallPath(callPath).withDebug(debug);
    }

    public static final class Builder {
        private String callerId;
        private String callReason;
        private List<String> callPath;
        private boolean debug;

        private Builder() {
        }

        public Builder withCallerId(String callerId) {
            this.callerId = callerId;
            return this;
        }

        public Builder withCallReason(String callReason) {
            this.callReason = callReason;
            return this;
        }

        public Builder withCallPath(List<String> callPath) {
            this.callPath = callPath;
            return this;
        }

        public Builder withDebug(boolean debug) {
            this.debug = debug;
            return this;
        }

        public Builder but() {
            return newBuilder().withCallerId(callerId).withCallReason(callReason).withCallPath(callPath).withDebug(debug);
        }

        public CallMetadata build() {
            return new CallMetadata(callerId, callReason == null ? "" : callReason, callPath == null ? Collections.emptyList() : callPath, debug);
        }
    }
}
