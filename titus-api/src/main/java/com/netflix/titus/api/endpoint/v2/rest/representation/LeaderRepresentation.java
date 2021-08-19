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

package com.netflix.titus.api.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LeaderRepresentation {

    private final String hostname;
    private final String hostIP;
    private final int apiPort;
    private final String apiStatusUri;
    private final long createTime;

    @JsonCreator
    public LeaderRepresentation(@JsonProperty("hostname") String hostname,
                                @JsonProperty("hostIP") String hostIP,
                                @JsonProperty("apiPort") int apiPort,
                                @JsonProperty("apiStatusUri") String apiStatusUri,
                                @JsonProperty("createTime") long createTime) {
        this.hostname = hostname;
        this.hostIP = hostIP;
        this.apiPort = apiPort;
        this.apiStatusUri = apiStatusUri;
        this.createTime = createTime;
    }

    public String getHostname() {
        return hostname;
    }

    public String getHostIP() {
        return hostIP;
    }

    public int getApiPort() {
        return apiPort;
    }

    public String getApiStatusUri() {
        return apiStatusUri;
    }

    public long getCreateTime() {
        return createTime;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String hostname;
        private String hostIP;
        private int apiPort;
        private String apiStatusUri;
        private long createTime;

        private Builder() {
        }

        public Builder withHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder withHostIP(String hostIP) {
            this.hostIP = hostIP;
            return this;
        }

        public Builder withApiPort(int apiPort) {
            this.apiPort = apiPort;
            return this;
        }

        public Builder withApiStatusUri(String apiStatusUri) {
            this.apiStatusUri = apiStatusUri;
            return this;
        }

        public Builder withCreateTime(long createTime) {
            this.createTime = createTime;
            return this;
        }

        public LeaderRepresentation build() {
            return new LeaderRepresentation(hostname, hostIP, apiPort, apiStatusUri, createTime);
        }
    }
}
