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

package com.netflix.titus.ext.k8s.clustermembership.connector.model;

import com.google.gson.annotations.SerializedName;

public class K8ClusterMembershipRevision {

    @SerializedName("current")
    private K8ClusterMember current;

    @SerializedName("code")
    private String code;

    @SerializedName("message")
    private String message;

    @SerializedName("timestamp")
    private long timestamp;

    public K8ClusterMember getCurrent() {
        return current;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public K8ClusterMembershipRevision current(K8ClusterMember current) {
        this.current = current;
        return this;
    }

    public K8ClusterMembershipRevision code(String code) {
        this.code = code;
        return this;
    }

    public K8ClusterMembershipRevision message(String message) {
        this.message = message;
        return this;
    }

    public K8ClusterMembershipRevision timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
