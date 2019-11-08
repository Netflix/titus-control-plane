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

public class AdmissionControllerResponse {

    private final boolean allowed;
    private final String reasonMessage;

    public AdmissionControllerResponse(boolean allowed, String reasonMessage) {
        this.allowed = allowed;
        this.reasonMessage = reasonMessage;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public String getReasonMessage() {
        return reasonMessage;
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
                Objects.equals(reasonMessage, that.reasonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowed, reasonMessage);
    }

    @Override
    public String toString() {
        return "AdmissionControllerResponse{" +
                "allowed=" + allowed +
                ", reasonMessage='" + reasonMessage + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean allowed;
        private String reasonMessage;

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

        public Builder but() {
            return newBuilder().withAllowed(allowed).withReasonMessage(reasonMessage);
        }

        public AdmissionControllerResponse build() {
            return new AdmissionControllerResponse(allowed, reasonMessage);
        }
    }
}
