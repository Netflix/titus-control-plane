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

import java.util.List;

public class AgentResourceCacheEni {
    private final List<String> ipAddresses;
    private final List<String> securityGroupIds;

    public AgentResourceCacheEni(List<String> ipAddresses, List<String> securityGroupIds) {
        this.ipAddresses = ipAddresses;
        this.securityGroupIds = securityGroupIds;
    }

    public List<String> getIpAddresses() {
        return ipAddresses;
    }

    public List<String> getSecurityGroupIds() {
        return securityGroupIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentResourceCacheEni that = (AgentResourceCacheEni) o;

        if (ipAddresses != null ? !ipAddresses.equals(that.ipAddresses) : that.ipAddresses != null) {
            return false;
        }
        return securityGroupIds != null ? securityGroupIds.equals(that.securityGroupIds) : that.securityGroupIds == null;
    }

    @Override
    public int hashCode() {
        int result = ipAddresses != null ? ipAddresses.hashCode() : 0;
        result = 31 * result + (securityGroupIds != null ? securityGroupIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentResourceCacheEni{" +
                "ipAddresses=" + ipAddresses +
                ", securityGroupIds=" + securityGroupIds +
                '}';
    }
}
