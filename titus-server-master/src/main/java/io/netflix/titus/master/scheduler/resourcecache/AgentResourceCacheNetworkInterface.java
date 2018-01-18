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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.Map;
import java.util.Set;

public class AgentResourceCacheNetworkInterface {
    private final int index;
    private final Map<String, Set<String>> ipAddresses;
    private final Set<String> securityGroupIds;
    private final boolean hasAvailableIps;
    private final String joinedSecurityGroupIds;
    private final long timestamp;

    public AgentResourceCacheNetworkInterface(int index,
                                              Map<String, Set<String>> ipAddresses,
                                              Set<String> securityGroupIds,
                                              boolean hasAvailableIps,
                                              String joinedSecurityGroupIds,
                                              long timestamp) {
        this.index = index;
        this.ipAddresses = ipAddresses;
        this.securityGroupIds = securityGroupIds;
        this.hasAvailableIps = hasAvailableIps;
        this.joinedSecurityGroupIds = joinedSecurityGroupIds;
        this.timestamp = timestamp;
    }

    public int getIndex() {
        return index;
    }

    public Map<String, Set<String>> getIpAddresses() {
        return ipAddresses;
    }

    public Set<String> getSecurityGroupIds() {
        return securityGroupIds;
    }

    public boolean hasAvailableIps() {
        return hasAvailableIps;
    }

    public String getJoinedSecurityGroupIds() {
        return joinedSecurityGroupIds;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentResourceCacheNetworkInterface that = (AgentResourceCacheNetworkInterface) o;

        if (index != that.index) {
            return false;
        }
        if (hasAvailableIps != that.hasAvailableIps) {
            return false;
        }
        if (timestamp != that.timestamp) {
            return false;
        }
        if (ipAddresses != null ? !ipAddresses.equals(that.ipAddresses) : that.ipAddresses != null) {
            return false;
        }
        if (securityGroupIds != null ? !securityGroupIds.equals(that.securityGroupIds) : that.securityGroupIds != null) {
            return false;
        }
        return joinedSecurityGroupIds != null ? joinedSecurityGroupIds.equals(that.joinedSecurityGroupIds) : that.joinedSecurityGroupIds == null;
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + (ipAddresses != null ? ipAddresses.hashCode() : 0);
        result = 31 * result + (securityGroupIds != null ? securityGroupIds.hashCode() : 0);
        result = 31 * result + (hasAvailableIps ? 1 : 0);
        result = 31 * result + (joinedSecurityGroupIds != null ? joinedSecurityGroupIds.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "AgentResourceCacheNetworkInterface{" +
                "index=" + index +
                ", ipAddresses=" + ipAddresses +
                ", securityGroupIds=" + securityGroupIds +
                ", hasAvailableIps=" + hasAvailableIps +
                ", joinedSecurityGroupIds='" + joinedSecurityGroupIds + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(AgentResourceCacheNetworkInterface eni) {
        return new Builder().withEniIndex(eni.getIndex())
                .withIpAddresses(eni.getIpAddresses())
                .withSecurityGroupIds(eni.getSecurityGroupIds())
                .withHasAvailableIps(eni.hasAvailableIps())
                .withJoinedSecurityGroupIds(eni.getJoinedSecurityGroupIds())
                .withTimestamp(eni.getTimestamp());
    }

    public static final class Builder {
        private int eniIndex;
        private Map<String, Set<String>> ipAddresses;
        private Set<String> securityGroupIds;
        private boolean hasAvailableIps;
        private String joinedSecurityGroupIds;
        private long timestamp;

        private Builder() {
        }

        public Builder withEniIndex(int eniIndex) {
            this.eniIndex = eniIndex;
            return this;
        }

        public Builder withIpAddresses(Map<String, Set<String>> ipAddresses) {
            this.ipAddresses = ipAddresses;
            return this;
        }

        public Builder withSecurityGroupIds(Set<String> securityGroupIds) {
            this.securityGroupIds = securityGroupIds;
            return this;
        }

        public Builder withHasAvailableIps(boolean hasAvailableIps) {
            this.hasAvailableIps = hasAvailableIps;
            return this;
        }

        public Builder withJoinedSecurityGroupIds(String joinedSecurityGroupIds) {
            this.joinedSecurityGroupIds = joinedSecurityGroupIds;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withEniIndex(eniIndex)
                    .withIpAddresses(ipAddresses)
                    .withSecurityGroupIds(securityGroupIds)
                    .withHasAvailableIps(hasAvailableIps)
                    .withJoinedSecurityGroupIds(joinedSecurityGroupIds)
                    .withTimestamp(timestamp);
        }

        public AgentResourceCacheNetworkInterface build() {
            return new AgentResourceCacheNetworkInterface(eniIndex, ipAddresses, securityGroupIds, hasAvailableIps, joinedSecurityGroupIds, timestamp);
        }
    }
}
