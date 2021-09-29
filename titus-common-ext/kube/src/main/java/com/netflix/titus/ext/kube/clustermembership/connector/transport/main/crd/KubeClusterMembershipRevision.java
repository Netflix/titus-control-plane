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

import com.google.gson.annotations.SerializedName;

public class KubeClusterMembershipRevision {

    @SerializedName("current")
    private KubeClusterMember current;

    @SerializedName("code")
    private String code;

    @SerializedName("message")
    private String message;

    @SerializedName("timestamp")
    private long timestamp;

    public KubeClusterMember getCurrent() {
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

    public KubeClusterMembershipRevision current(KubeClusterMember current) {
        this.current = current;
        return this;
    }

    public KubeClusterMembershipRevision code(String code) {
        this.code = code;
        return this;
    }

    public KubeClusterMembershipRevision message(String message) {
        this.message = message;
        return this;
    }

    public KubeClusterMembershipRevision timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
