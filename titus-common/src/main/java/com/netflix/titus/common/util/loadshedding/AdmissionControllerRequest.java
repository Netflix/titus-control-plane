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

public class AdmissionControllerRequest {

    private final String endpointName;
    private final String callerId;
    private final int hash;

    private AdmissionControllerRequest(String endpointName, String callerId) {
        this.endpointName = endpointName;
        this.callerId = callerId;
        this.hash = Objects.hash(endpointName, callerId);
    }

    public String getEndpointName() {
        return endpointName;
    }

    public String getCallerId() {
        return callerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdmissionControllerRequest that = (AdmissionControllerRequest) o;
        return Objects.equals(endpointName, that.endpointName) &&
                Objects.equals(callerId, that.callerId);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "AdmissionControllerRequest{" +
                "endpointName='" + endpointName + '\'' +
                ", callerId='" + callerId + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String endpointName;
        private String callerId;

        private Builder() {
        }

        public Builder withEndpointName(String endpointName) {
            this.endpointName = endpointName;
            return this;
        }

        public Builder withCallerId(String callerId) {
            this.callerId = callerId;
            return this;
        }

        public Builder but() {
            return newBuilder().withEndpointName(endpointName).withCallerId(callerId);
        }

        public AdmissionControllerRequest build() {
            return new AdmissionControllerRequest(endpointName, callerId);
        }
    }
}
