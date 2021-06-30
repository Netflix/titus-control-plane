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

package com.netflix.titus.api.jobmanager.model.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class NetworkConfiguration {

    final private int networkMode;

    public NetworkConfiguration(int networkMode) {
        this.networkMode = networkMode;
    }

    public int getNetworkMode() { return networkMode; }

    @JsonIgnore
    public String getNetworkModeName() {
        return networkModeToName(networkMode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NetworkConfiguration netConfig = (NetworkConfiguration) o;
        return networkMode == netConfig.networkMode;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(NetworkConfiguration networkConfiguration) {
        return new Builder()
                .withNetworkMode(networkConfiguration.getNetworkMode());
    }

    public static final class Builder {
        private int networkMode;

        private Builder() {
        }

        public Builder withNetworkMode(int networkMode) {
            this.networkMode = networkMode;
            return this;
        }

        public NetworkConfiguration build() {
            return new NetworkConfiguration(networkMode);
        }
    }

    public static String networkModeToName(int mode) {
        // TODO: Use the generated protobuf version of this function if available?
        switch (mode) {
            case 0: return "UnknownNetworkMode";
            case 1: return "Ipv4Only";
            case 2: return "Ipv6AndIpv4";
            case 3: return "Ipv6AndIpv4Fallback";
            case 4: return "Ipv6Only";
            default: return "";
        }
    }
}
