/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.util.loadshedding;

import java.util.Objects;

import com.netflix.titus.common.util.Evaluators;

public class AdmissionControllerResponse {

    /**
     * Set to true, if request can be executed, and false if it should be discarded.
     */
    private final boolean allowed;

    /**
     * Human readable explanation of the result.
     */
    private final String reasonMessage;

    /**
     * The name of a {@link AdmissionController} making the final decision.
     */
    private final String decisionPoint;

    /**
     * All requests within the equivalence group are regarded as identical, and share the rate limits.
     */
    private final String equivalenceGroup;

    public AdmissionControllerResponse(boolean allowed,
                                       String reasonMessage,
                                       String decisionPoint,
                                       String equivalenceGroup) {
        this.allowed = allowed;
        this.reasonMessage = Evaluators.getOrDefault(reasonMessage, "");
        this.decisionPoint = Evaluators.getOrDefault(decisionPoint, "");
        this.equivalenceGroup = Evaluators.getOrDefault(equivalenceGroup, "");
    }

    public boolean isAllowed() {
        return allowed;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public String getDecisionPoint() {
        return decisionPoint;
    }

    public String getEquivalenceGroup() {
        return equivalenceGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdmissionControllerResponse that = (AdmissionControllerResponse) o;
        return allowed == that.allowed &&
                Objects.equals(reasonMessage, that.reasonMessage) &&
                Objects.equals(decisionPoint, that.decisionPoint) &&
                Objects.equals(equivalenceGroup, that.equivalenceGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowed, reasonMessage, decisionPoint, equivalenceGroup);
    }

    @Override
    public String toString() {
        return "AdmissionControllerResponse{" +
                "allowed=" + allowed +
                ", reasonMessage='" + reasonMessage + '\'' +
                ", decisionPoint='" + decisionPoint + '\'' +
                ", equivalenceGroup='" + equivalenceGroup + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withAllowed(allowed).withReasonMessage(reasonMessage).withDecisionPoint(decisionPoint).withEquivalenceGroup(equivalenceGroup);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean allowed;
        private String reasonMessage;
        private String decisionPoint;
        private String equivalenceGroup;

        private Builder() {
        }

        public Builder withAllowed(boolean allowed) {
            this.allowed = allowed;
            return this;
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public Builder withDecisionPoint(String decisionPoint) {
            this.decisionPoint = decisionPoint;
            return this;
        }

        public Builder withEquivalenceGroup(String equivalenceGroup) {
            this.equivalenceGroup = equivalenceGroup;
            return this;
        }

        public AdmissionControllerResponse build() {
            return new AdmissionControllerResponse(allowed, reasonMessage, decisionPoint, equivalenceGroup);
        }
    }
}
