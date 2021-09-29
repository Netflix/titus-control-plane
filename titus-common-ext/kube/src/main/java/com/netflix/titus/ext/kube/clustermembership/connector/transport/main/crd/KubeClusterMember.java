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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class KubeClusterMember {

    @SerializedName("memberId")
    private String memberId;

    @SerializedName("enabled")
    private boolean enabled;

    @SerializedName("active")
    private boolean active;

    @SerializedName("clusterMemberAddress")
    private List<KubeClusterMemberAddress> clusterMemberAddress;

    @SerializedName("labels")
    private Map<String, String> labels;

    public String getMemberId() {
        return memberId;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isActive() {
        return active;
    }

    public List<KubeClusterMemberAddress> getClusterMemberAddress() {
        return clusterMemberAddress;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public KubeClusterMember memberId(String memberId) {
        this.memberId = memberId;
        return this;
    }

    public KubeClusterMember enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public KubeClusterMember active(boolean active) {
        this.active = active;
        return this;
    }

    public KubeClusterMember clusterMemberAddress(List<KubeClusterMemberAddress> clusterMemberAddress) {
        this.clusterMemberAddress = clusterMemberAddress;
        return this;
    }

    public KubeClusterMember labels(Map<String, String> labels) {
        this.labels = labels;
        return this;
    }

}
