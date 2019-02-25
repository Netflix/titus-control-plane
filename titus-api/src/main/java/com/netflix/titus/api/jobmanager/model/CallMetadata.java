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

package com.netflix.titus.api.jobmanager.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

public class CallMetadata {

    /**
     * @deprecated Use {@link #callers} instead.
     */
    @Deprecated
    private final String callerId;

    private final String callReason;

    /**
     * @deprecated Use {@link #callers} instead.
     */
    @Deprecated
    private final List<String> callPath;

    private final List<Caller> callers;

    private final boolean debug;

    public CallMetadata(String callerId,
                        String callReason,
                        List<String> callPath,
                        List<Caller> callers,
                        boolean debug) {
        this.callerId = callerId;
        this.callReason = callReason;
        this.callPath = callPath;
        this.callers = callers;
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

    public List<Caller> getCallers() {
        return callers;
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
                Objects.equals(callPath, that.callPath) &&
                Objects.equals(callers, that.callers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(callerId, callReason, callPath, callers, debug);
    }

    @Override
    public String toString() {
        return "CallMetadata{" +
                "callerId='" + callerId + '\'' +
                ", callReason='" + callReason + '\'' +
                ", callPath=" + callPath +
                ", callers=" + callers +
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
        private List<Caller> callers;

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

        public Builder withCallers(List<Caller> callers) {
            this.callers = callers;
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
            this.callReason = StringExt.safeTrim(callReason);

            if (!CollectionsExt.isNullOrEmpty(callers)) {
                return buildNew();
            }

            // Legacy
            Preconditions.checkNotNull(callerId, "The original caller id not");

            List<Caller> callersFromLegacy = Collections.singletonList(
                    Caller.newBuilder()
                            .withId(callerId)
                            .withCallerType(CallerType.parseCallerType(callerId, ""))
                            .build()
            );

            return new CallMetadata(callerId, callReason, CollectionsExt.nonNull(callPath), callersFromLegacy, debug);
        }

        /**
         * Populates deprecated fields from the callers.
         */
        private CallMetadata buildNew() {
            Caller originalCaller = callers.get(0);
            return new CallMetadata(
                    originalCaller.getId(),
                    callReason,
                    callers.stream().map(Caller::getId).collect(Collectors.toList()),
                    callers,
                    debug
            );
        }
    }
}
