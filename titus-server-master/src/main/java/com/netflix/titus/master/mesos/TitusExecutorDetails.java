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

package com.netflix.titus.master.mesos;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Titus executor data model for reporting back resource allocation data.
 */
public class TitusExecutorDetails {

    private final Map<String, String> ipAddresses;
    private final NetworkConfiguration networkConfiguration;

    public TitusExecutorDetails(
            @JsonProperty("ipAddresses") Map<String, String> ipAddresses,
            @JsonProperty("NetworkConfiguration") NetworkConfiguration networkConfiguration) {
        this.ipAddresses = ipAddresses;
        this.networkConfiguration = networkConfiguration;
    }

    public Map<String, String> getIpAddresses() {
        return ipAddresses;
    }

    @JsonProperty("NetworkConfiguration")
    public NetworkConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TitusExecutorDetails that = (TitusExecutorDetails) o;

        if (ipAddresses != null ? !ipAddresses.equals(that.ipAddresses) : that.ipAddresses != null) {
            return false;
        }
        return networkConfiguration != null ? networkConfiguration.equals(that.networkConfiguration) : that.networkConfiguration == null;
    }

    @Override
    public int hashCode() {
        int result = ipAddresses != null ? ipAddresses.hashCode() : 0;
        result = 31 * result + (networkConfiguration != null ? networkConfiguration.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TitusExecutorDetails{" +
                "ipAddresses=" + ipAddresses +
                ", networkConfiguration=" + networkConfiguration +
                '}';
    }

    public static class NetworkConfiguration {

        private final boolean isRoutableIP;

        private final String ipAddress;

        private final String eniIPAddress;

        private final String eniID;

        private final String resourceID;

        public NetworkConfiguration(
                @JsonProperty("IsRoutableIP") boolean isRoutableIP,
                @JsonProperty("IPAddress") String ipAddress,
                @JsonProperty("EniIPAddress") String eniIPAddress,
                @JsonProperty("EniID") String eniID,
                @JsonProperty("ResourceID") String resourceID) {
            this.isRoutableIP = isRoutableIP;
            this.ipAddress = ipAddress;
            this.eniIPAddress = eniIPAddress;
            this.eniID = eniID;
            this.resourceID = resourceID;
        }

        @JsonProperty("IsRoutableIP")
        public boolean isRoutableIP() {
            return isRoutableIP;
        }

        @JsonProperty("IPAddress")
        public String getIpAddress() {
            return ipAddress;
        }

        @JsonProperty("EniIPAddress")
        public String getEniIPAddress() {
            return eniIPAddress;
        }

        @JsonProperty("EniID")
        public String getEniID() {
            return eniID;
        }

        @JsonProperty("ResourceID")
        public String getResourceID() {
            return resourceID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NetworkConfiguration that = (NetworkConfiguration) o;

            if (isRoutableIP != that.isRoutableIP) {
                return false;
            }
            if (ipAddress != null ? !ipAddress.equals(that.ipAddress) : that.ipAddress != null) {
                return false;
            }
            if (eniIPAddress != null ? !eniIPAddress.equals(that.eniIPAddress) : that.eniIPAddress != null) {
                return false;
            }
            if (eniID != null ? !eniID.equals(that.eniID) : that.eniID != null) {
                return false;
            }
            return resourceID != null ? resourceID.equals(that.resourceID) : that.resourceID == null;
        }

        @Override
        public int hashCode() {
            int result = (isRoutableIP ? 1 : 0);
            result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
            result = 31 * result + (eniIPAddress != null ? eniIPAddress.hashCode() : 0);
            result = 31 * result + (eniID != null ? eniID.hashCode() : 0);
            result = 31 * result + (resourceID != null ? resourceID.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "NetworkConfiguration{" +
                    "isRoutableIP=" + isRoutableIP +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", eniIPAddress='" + eniIPAddress + '\'' +
                    ", eniID='" + eniID + '\'' +
                    ", resourceID='" + resourceID + '\'' +
                    '}';
        }
    }
}
