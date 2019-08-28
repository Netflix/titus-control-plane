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

package com.netflix.titus.master.mesos.kubeapiserver.model.v1;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1ObjectMeta;

/**
 * GSON-compatible POJO, with a default constructor following JavaBeans conventions.
 */
// TODO(fabio): autogenerate from the CRD swagger definition
public class V1OpportunisticResource {
    private static final String LABEL_INSTANCE_ID = "node_name";
    private static final String UNKNOWN_INSTANCE_ID = "unknown";

    @SerializedName("apiVersion")
    private String apiVersion;

    @SerializedName("kind")
    private String kind;

    @SerializedName("metadata")
    private V1ObjectMeta metadata;

    @SerializedName("spec")
    private V1OpportunisticResourceSpec spec;

    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V1ObjectMeta getMetadata() {
        return metadata;
    }

    public void setMetadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public V1OpportunisticResourceSpec getSpec() {
        return spec;
    }

    public void setSpec(V1OpportunisticResourceSpec spec) {
        this.spec = spec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        V1OpportunisticResource that = (V1OpportunisticResource) o;
        return Objects.equals(apiVersion, that.apiVersion) &&
                Objects.equals(kind, that.kind) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(spec, that.spec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiVersion, kind, metadata, spec);
    }

    @Override
    public String toString() {
        return "V1OpportunisticResource{" +
                "apiVersion='" + apiVersion + '\'' +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                ", spec=" + spec +
                '}';
    }

    public String getUid() {
        if (metadata == null || metadata.getUid() == null) {
            return "";
        }
        return metadata.getUid();
    }

    public String getInstanceId() {
        return getLabels().getOrDefault(LABEL_INSTANCE_ID, UNKNOWN_INSTANCE_ID);
    }

    private Map<String, String> getLabels() {
        if (metadata == null || metadata.getLabels() == null) {
            return Collections.emptyMap();
        }
        return metadata.getLabels();
    }

    public Instant getEnd() {
        if (spec == null || spec.getWindow() == null || spec.getWindow().getEnd() == null) {
            return Instant.EPOCH;
        }
        return Instant.ofEpochMilli(spec.getWindow().getEnd().getMillis());
    }

    public int getCpus() {
        if (spec == null || spec.getCapacity() == null) {
            return 0;
        }
        return spec.getCapacity().getCpu();
    }
}
