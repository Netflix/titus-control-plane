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
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1StatusDetails;

/**
 * {@link io.kubernetes.client.openapi.models.V1Status} and {@link io.kubernetes.client.openapi.models.V1ListMeta} are incomplete.
 */
public class KubeStatus {

    @SerializedName("apiVersion")
    private String apiVersion = null;

    @SerializedName("code")
    private Integer code = null;

    @SerializedName("details")
    private V1StatusDetails details = null;

    @SerializedName("kind")
    private String kind = null;

    @SerializedName("message")
    private String message = null;

    @SerializedName("metadata")
    private V1ObjectMeta metadata = null;

    @SerializedName("reason")
    private String reason = null;

    @SerializedName("status")
    private String status = null;

    @SerializedName("spec")
    private Object spec;

    public String getApiVersion() {
        return apiVersion;
    }

    public Integer getCode() {
        return code;
    }

    public V1StatusDetails getDetails() {
        return details;
    }

    public String getKind() {
        return kind;
    }

    public String getMessage() {
        return message;
    }

    public V1ObjectMeta getMetadata() {
        return metadata;
    }

    public String getReason() {
        return reason;
    }

    public String getStatus() {
        return status;
    }

    public Object getSpec() {
        return spec;
    }

    public KubeStatus apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public KubeStatus code(int code) {
        this.code = code;
        return this;
    }

    public KubeStatus kind(String kind) {
        this.kind = kind;
        return this;
    }

    public KubeStatus message(String message) {
        this.message = message;
        return this;
    }

    public KubeStatus metadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public KubeStatus reason(String reason) {
        this.reason = reason;
        return this;
    }

    public KubeStatus status(String status) {
        this.status = status;
        return this;
    }

    public KubeStatus spec(Object spec) {
        this.spec = spec;
        return this;
    }
}
