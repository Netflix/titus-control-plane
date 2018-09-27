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

package com.netflix.titus.api.agent.model;

import java.util.Comparator;
import java.util.Map;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.model.sanitizer.CollectionInvariants;

@ClassFieldsNotNull
public class AgentInstance {

    private static final Comparator<AgentInstance> ID_COMPARATOR = Comparator.comparing(AgentInstance::getId);

    private final String id;

    private final String instanceGroupId;

    private final String ipAddress;

    private final String hostname;

    private final InstanceLifecycleStatus lifecycleStatus;

    @CollectionInvariants
    private final Map<String, String> attributes;

    public AgentInstance(String id,
                         String instanceGroupId,
                         String ipAddress,
                         String hostname,
                         InstanceLifecycleStatus lifecycleStatus,
                         Map<String, String> attributes) {
        this.id = id;
        this.instanceGroupId = instanceGroupId;
        this.ipAddress = ipAddress;
        this.hostname = hostname;
        this.lifecycleStatus = lifecycleStatus;
        this.attributes = attributes;
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

    public InstanceLifecycleStatus getLifecycleStatus() {
        return lifecycleStatus;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentInstance that = (AgentInstance) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (instanceGroupId != null ? !instanceGroupId.equals(that.instanceGroupId) : that.instanceGroupId != null) {
            return false;
        }
        if (ipAddress != null ? !ipAddress.equals(that.ipAddress) : that.ipAddress != null) {
            return false;
        }
        if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) {
            return false;
        }
        if (lifecycleStatus != null ? !lifecycleStatus.equals(that.lifecycleStatus) : that.lifecycleStatus != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (instanceGroupId != null ? instanceGroupId.hashCode() : 0);
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        result = 31 * result + (lifecycleStatus != null ? lifecycleStatus.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentInstance{" +
                "id='" + id + '\'' +
                ", instanceGroupId='" + instanceGroupId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", hostname='" + hostname + '\'' +
                ", lifecycleStatus=" + lifecycleStatus +
                ", attributes=" + attributes +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withId(id)
                .withInstanceGroupId(instanceGroupId)
                .withIpAddress(ipAddress)
                .withHostname(hostname)
                .withDeploymentStatus(lifecycleStatus)
                .withAttributes(attributes);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Comparator<AgentInstance> idComparator() {
        return ID_COMPARATOR;
    }

    public static final class Builder {
        private String id;
        private String instanceGroupId;
        private String ipAddress;
        private String hostname;
        private InstanceLifecycleStatus instanceLifecycleStatus;
        private Map<String, String> attributes;
        private long timestamp;

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

        public Builder withDeploymentStatus(InstanceLifecycleStatus instanceLifecycleStatus) {
            this.instanceLifecycleStatus = instanceLifecycleStatus;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withId(id).withInstanceGroupId(instanceGroupId).withIpAddress(ipAddress).withHostname(hostname).withDeploymentStatus(instanceLifecycleStatus).withAttributes(attributes).withTimestamp(timestamp);
        }

        public AgentInstance build() {
            AgentInstance agentInstance = new AgentInstance(id, instanceGroupId, ipAddress, hostname, instanceLifecycleStatus, attributes);
            return agentInstance;
        }
    }
}
