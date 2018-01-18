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

public class AgentResourceCacheInstance {
    private final String hostname;
    private final Set<AgentResourceCacheImage> images;
    private final Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces;

    public AgentResourceCacheInstance(String hostname, Set<AgentResourceCacheImage> images, Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces) {
        this.hostname = hostname;
        this.images = images;
        this.networkInterfaces = networkInterfaces;
    }

    public String getHostname() {
        return hostname;
    }

    public Set<AgentResourceCacheImage> getImages() {
        return images;
    }

    public Map<Integer, AgentResourceCacheNetworkInterface> getNetworkInterfaces() {
        return networkInterfaces;
    }

    public AgentResourceCacheNetworkInterface getNetworkInterface(Integer index) {
        return networkInterfaces.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentResourceCacheInstance instance = (AgentResourceCacheInstance) o;

        if (hostname != null ? !hostname.equals(instance.hostname) : instance.hostname != null) {
            return false;
        }
        if (images != null ? !images.equals(instance.images) : instance.images != null) {
            return false;
        }
        return networkInterfaces != null ? networkInterfaces.equals(instance.networkInterfaces) : instance.networkInterfaces == null;
    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + (images != null ? images.hashCode() : 0);
        result = 31 * result + (networkInterfaces != null ? networkInterfaces.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentResourceCacheInstance{" +
                "hostname='" + hostname + '\'' +
                ", images=" + images +
                ", networkInterfaces=" + networkInterfaces +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(AgentResourceCacheInstance instance) {
        return newBuilder().withHostname(instance.getHostname())
                .withImages(instance.getImages())
                .withNetworkInterfaces(instance.getNetworkInterfaces());
    }

    public static final class Builder {
        private String hostname;
        private Set<AgentResourceCacheImage> images;
        private Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces;

        private Builder() {
        }

        public Builder withHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder withImages(Set<AgentResourceCacheImage> images) {
            this.images = images;
            return this;
        }

        public Builder withNetworkInterfaces(Map<Integer, AgentResourceCacheNetworkInterface> enis) {
            this.networkInterfaces = enis;
            return this;
        }

        public Builder but() {
            return newBuilder().withHostname(hostname).withImages(images).withNetworkInterfaces(networkInterfaces);
        }

        public AgentResourceCacheInstance build() {
            return new AgentResourceCacheInstance(hostname, images, networkInterfaces);
        }
    }
}
