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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AgentResourceCacheNetworkInterface {
    private final int index;
    private final Map<String, Set<String>> ipAddresses;
    private final Set<String> securityGroupIds;
    private final boolean hasAvailableIps;
    private final long timestamp;

    public AgentResourceCacheNetworkInterface(int index,
                                              Map<String, Set<String>> ipAddresses,
                                              Set<String> securityGroupIds,
                                              boolean hasAvailableIps,
                                              long timestamp) {
        this.index = index;
        this.ipAddresses = ipAddresses;
        this.securityGroupIds = securityGroupIds;
        this.hasAvailableIps = hasAvailableIps;
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
        return index == that.index &&
                hasAvailableIps == that.hasAvailableIps &&
                timestamp == that.timestamp &&
                Objects.equals(ipAddresses, that.ipAddresses) &&
                Objects.equals(securityGroupIds, that.securityGroupIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, ipAddresses, securityGroupIds, hasAvailableIps, timestamp);
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
                .withTimestamp(eni.getTimestamp());
    }

    public static final class Builder {
        private int eniIndex;
        private Map<String, Set<String>> ipAddresses;
        private Set<String> securityGroupIds;
        private boolean hasAvailableIps;
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

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withEniIndex(eniIndex)
                    .withIpAddresses(ipAddresses)
                    .withSecurityGroupIds(securityGroupIds)
                    .withHasAvailableIps(hasAvailableIps)
                    .withTimestamp(timestamp);
        }

        public AgentResourceCacheNetworkInterface build() {
            return new AgentResourceCacheNetworkInterface(eniIndex, ipAddresses, securityGroupIds, hasAvailableIps, timestamp);
        }
    }
}
