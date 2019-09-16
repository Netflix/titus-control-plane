/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector.model;

import com.google.gson.annotations.SerializedName;

public class KubeClusterMemberAddress {

    @SerializedName("ipAddress")
    private String ipAddress;

    @SerializedName("portNumber")
    private int portNumber;

    @SerializedName("protocol")
    private String protocol;

    @SerializedName("secure")
    private boolean secure;

    @SerializedName("description")
    private String description;

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public String getProtocol() {
        return protocol;
    }

    public boolean isSecure() {
        return secure;
    }

    public String getDescription() {
        return description;
    }

    KubeClusterMemberAddress ipAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }

    KubeClusterMemberAddress portNumber(int portNumber) {
        this.portNumber = portNumber;
        return this;
    }

    KubeClusterMemberAddress protocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    KubeClusterMemberAddress secure(boolean secure) {
        this.secure = secure;
        return this;
    }

    KubeClusterMemberAddress description(String description) {
        this.description = description;
        return this;
    }
}
