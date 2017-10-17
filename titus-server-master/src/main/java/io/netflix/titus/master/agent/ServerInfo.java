/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Entity describing server resources.
 */
public class ServerInfo {

    private final String serverType;
    private final int cpus;
    private final int gpus;
    private final int memoryGB;
    private final int storageGB;
    private final int networkMbs;
    private final Map<String, String> attributes;

    @JsonCreator
    public ServerInfo(@JsonProperty("serverType") String serverType,
                      @JsonProperty("cpus") int cpus,
                      @JsonProperty("gpus") int gpus,
                      @JsonProperty("memoryGB") int memoryGB,
                      @JsonProperty("storageGB") int storageGB,
                      @JsonProperty("networkMbs") int networkMbs,
                      @JsonProperty("attributes") Map<String, String> attributes) {
        this.serverType = serverType;
        this.cpus = cpus;
        this.gpus = gpus;
        this.memoryGB = memoryGB;
        this.storageGB = storageGB;
        this.networkMbs = networkMbs;
        this.attributes = Collections.unmodifiableMap(new HashMap<>(attributes));
    }

    public String getServerType() {
        return serverType;
    }

    public int getCpus() {
        return cpus;
    }

    public int getGpus() {
        return gpus;
    }

    public int getMemoryGB() {
        return memoryGB;
    }

    public int getStorageGB() {
        return storageGB;
    }

    public int getNetworkMbs() {
        return networkMbs;
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

        ServerInfo that = (ServerInfo) o;

        if (cpus != that.cpus) {
            return false;
        }
        if (memoryGB != that.memoryGB) {
            return false;
        }
        if (storageGB != that.storageGB) {
            return false;
        }
        if (networkMbs != that.networkMbs) {
            return false;
        }
        if (serverType != null ? !serverType.equals(that.serverType) : that.serverType != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;

    }

    @Override
    public int hashCode() {
        int result = serverType != null ? serverType.hashCode() : 0;
        result = 31 * result + cpus;
        result = 31 * result + memoryGB;
        result = 31 * result + storageGB;
        result = 31 * result + networkMbs;
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ServerInfo{" +
                "serverType='" + serverType + '\'' +
                ", cpus=" + cpus +
                ", memoryGB=" + memoryGB +
                ", storageGB=" + storageGB +
                ", networkMbs=" + networkMbs +
                ", attributes=" + attributes +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String serverType;
        private int cpus;
        private int gpus;
        private int memoryGB;
        private int storageGB;
        private int networkMbs;
        private Map<String, String> attributes;

        private Builder() {
        }

        public Builder withServerType(String serverType) {
            this.serverType = serverType;
            return this;
        }

        public Builder withCpus(int cpus) {
            this.cpus = cpus;
            return this;
        }

        public Builder withGpus(int gpus) {
            this.gpus = gpus;
            return this;
        }

        public Builder withMemoryGB(int memoryGB) {
            this.memoryGB = memoryGB;
            return this;
        }

        public Builder withStorageGB(int storageGB) {
            this.storageGB = storageGB;
            return this;
        }

        public Builder withNetworkMbs(int networkMbs) {
            this.networkMbs = networkMbs;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = new HashMap<>(attributes);
            return this;
        }

        public Builder withAttribute(String key, String value) {
            if (attributes == null) {
                attributes = new HashMap<>();
            }
            attributes.put(key, value);
            return this;
        }

        public Builder but() {
            return newBuilder().withServerType(serverType).withCpus(cpus).withMemoryGB(memoryGB).withStorageGB(storageGB).withNetworkMbs(networkMbs);
        }

        public ServerInfo build() {
            Map<String, String> attributesOrEmpty = attributes == null ? Collections.emptyMap() : attributes;
            return new ServerInfo(serverType, cpus, gpus, memoryGB, storageGB, networkMbs, attributesOrEmpty);
        }
    }
}

