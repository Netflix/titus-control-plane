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

package com.netflix.titus.api.connector.cloud;

import java.util.Map;

public class Instance {

    public enum InstanceState {
        Unknown,
        Starting,
        Running,
        Terminating,
        Terminated,
        Stopping,
        Stopped,
    }

    private final String id;
    private final String instanceGroupId;
    private final String ipAddress;
    private final String hostname;
    private final InstanceState instanceState;
    private final Map<String, String> attributes;
    private final long launchTime;

    private Instance(String id,
                     String instanceGroupId,
                     String ipAddress,
                     String hostname,
                     InstanceState instanceState,
                     Map<String, String> attributes,
                     long launchTime) {
        this.id = id;
        this.instanceGroupId = instanceGroupId;
        this.ipAddress = ipAddress;
        this.hostname = hostname;
        this.instanceState = instanceState;
        this.attributes = attributes;
        this.launchTime = launchTime;
    }

    public String getId() {
        return id;
    }

    public String getInstanceGroupId() {
        return instanceGroupId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getHostname() {
        return hostname;
    }

    public InstanceState getInstanceState() {
        return instanceState;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public long getLaunchTime() {
        return launchTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Instance instance = (Instance) o;

        if (launchTime != instance.launchTime) {
            return false;
        }
        if (id != null ? !id.equals(instance.id) : instance.id != null) {
            return false;
        }
        if (instanceGroupId != null ? !instanceGroupId.equals(instance.instanceGroupId) : instance.instanceGroupId != null) {
            return false;
        }
        if (ipAddress != null ? !ipAddress.equals(instance.ipAddress) : instance.ipAddress != null) {
            return false;
        }
        if (hostname != null ? !hostname.equals(instance.hostname) : instance.hostname != null) {
            return false;
        }
        if (instanceState != instance.instanceState) {
            return false;
        }
        return attributes != null ? attributes.equals(instance.attributes) : instance.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (instanceGroupId != null ? instanceGroupId.hashCode() : 0);
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        result = 31 * result + (instanceState != null ? instanceState.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (int) (launchTime ^ (launchTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Instance{" +
                "id='" + id + '\'' +
                ", instanceGroupId='" + instanceGroupId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", hostname='" + hostname + '\'' +
                ", instanceState=" + instanceState +
                ", attributes=" + attributes +
                ", launchTime=" + launchTime +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withInstanceGroupId(instanceGroupId).withIpAddress(ipAddress).withHostname(hostname)
                .withInstanceState(instanceState).withAttributes(attributes).withLaunchTime(launchTime);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String instanceGroupId;
        private String ipAddress;
        private String hostname;
        private InstanceState instanceState;
        private Map<String, String> attributes;
        private long launchTime;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withInstanceGroupId(String instanceGroupId) {
            this.instanceGroupId = instanceGroupId;
            return this;
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder withInstanceState(InstanceState instanceState) {
            this.instanceState = instanceState;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withLaunchTime(long launchTime) {
            this.launchTime = launchTime;
            return this;
        }

        public Builder but() {
            return newBuilder().withId(id).withInstanceGroupId(instanceGroupId).withIpAddress(ipAddress)
                    .withHostname(hostname).withInstanceState(instanceState).withAttributes(attributes)
                    .withLaunchTime(launchTime);
        }

        public Instance build() {
            return new Instance(id, instanceGroupId, ipAddress, hostname, instanceState, attributes, launchTime);
        }
    }
}
