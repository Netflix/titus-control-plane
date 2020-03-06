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
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class KubeClusterMembershipRevisionResource {

    @SerializedName("apiVersion")
    private String apiVersion = null;

    @SerializedName("kind")
    private String kind = null;

    @SerializedName("metadata")
    private V1ObjectMeta metadata = null;

    @SerializedName("spec")
    private KubeClusterMembershipRevision spec = null;

    public String getApiVersion() {
        return apiVersion;
    }

    public String getKind() {
        return kind;
    }

    public V1ObjectMeta getMetadata() {
        return metadata;
    }

    public KubeClusterMembershipRevision getSpec() {
        return spec;
    }

    public KubeClusterMembershipRevisionResource apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public KubeClusterMembershipRevisionResource kind(String kind) {
        this.kind = kind;
        return this;
    }

    public KubeClusterMembershipRevisionResource metadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public KubeClusterMembershipRevisionResource spec(KubeClusterMembershipRevision spec) {
        this.spec = spec;
        return this;
    }
}
