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

package com.netflix.titus.api.clustermembership.model;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.Evaluators;

public class ClusterMemberAddress {

    private final String ipAddress;
    private final int portNumber;
    private final String protocol;
    private final boolean secure;
    private final String description;

    public ClusterMemberAddress(String ipAddress, int portNumber, String protocol, boolean secure, String description) {
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
        this.protocol = protocol;
        this.secure = secure;
        this.description = Evaluators.getOrDefault(description, "");
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public String getProtocol() {
        return protocol;
    }

    public boolean isSecure() {
        return secure;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMemberAddress that = (ClusterMemberAddress) o;
        return portNumber == that.portNumber &&
                secure == that.secure &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(protocol, that.protocol) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, portNumber, protocol, secure, description);
    }

    @Override
    public String toString() {
        return "ClusterMemberAddress{" +
                "ipAddress='" + ipAddress + '\'' +
                ", portNumber=" + portNumber +
                ", protocol='" + protocol + '\'' +
                ", secure=" + secure +
                ", description='" + description + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withIpAddress(ipAddress).withPortNumber(portNumber).withProtocol(protocol).withSecure(secure).withDescription(description);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String ipAddress;
        private int portNumber;
        private String protocol;
        private boolean secure;
        private String description;

        private Builder() {
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withPortNumber(int portNumber) {
            this.portNumber = portNumber;
            return this;
        }

        public Builder withProtocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder withSecure(boolean secure) {
            this.secure = secure;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public ClusterMemberAddress build() {
            Preconditions.checkNotNull(ipAddress, "IP address not set");
            Preconditions.checkNotNull(protocol, "Protocol not set");

            return new ClusterMemberAddress(ipAddress, portNumber, protocol, secure, description);
        }
    }
}
