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

package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CallMetadata {

    private final String callerId;
    private final CallerType callerType;
    private final String callReason;
    private final List<String> callPath;
    private final boolean debug;

    public CallMetadata(String callerId, CallerType callerType, String callReason, List<String> callPath, boolean debug) {
        this.callerId = callerId;
        this.callerType = callerType;
        this.callReason = callReason;
        this.callPath = callPath;
        this.debug = debug;
    }

    public String getCallerId() {
        return callerId;
    }

    public CallerType getCallerType() {
        return callerType;
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
                callerType == that.callerType &&
                Objects.equals(callReason, that.callReason) &&
                Objects.equals(callPath, that.callPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(callerId, callerType, callReason, callPath, debug);
    }

    @Override
    public String toString() {
        return "CallMetadata{" +
                "callerId='" + callerId + '\'' +
                ", callerType=" + callerType +
                ", callReason='" + callReason + '\'' +
                ", callPath=" + callPath +
                ", debug=" + debug +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return newBuilder().withCallerId(callerId).withCallerType(callerType).withCallReason(callReason).withCallPath(callPath).withDebug(debug);
    }

    public static final class Builder {
        private String callerId;
        private CallerType callerType;
        private String callReason;
        private List<String> callPath;
        private boolean debug;

        private Builder() {
        }

        public Builder withCallerId(String callerId) {
            this.callerId = callerId;
            return this;
        }

        public Builder withCallerType(CallerType callerType) {
            this.callerType = callerType;
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
            return newBuilder().withCallerId(callerId).withCallerType(callerType).withCallReason(callReason).withCallPath(callPath).withDebug(debug);
        }

        public CallMetadata build() {
            return new CallMetadata(callerId,
                    callerType == null ? CallerType.Unknown : callerType,
                    callReason == null ? "" : callReason,
                    callPath == null ? Collections.emptyList() : callPath,
                    debug
            );
        }
    }
}
