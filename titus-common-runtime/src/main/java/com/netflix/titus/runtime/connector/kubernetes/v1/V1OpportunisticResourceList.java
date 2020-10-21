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

package com.netflix.titus.runtime.connector.kubernetes.v1;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.openapi.models.V1ListMeta;

/**
 * GSON-compatible POJO, with a default constructor following JavaBeans conventions.
 */
// TODO(fabio): autogenerate from the CRD swagger definition
public class V1OpportunisticResourceList implements KubernetesListObject {
    @SerializedName("apiVersion")
    private String apiVersion = null;

    @SerializedName("items")
    private List<V1OpportunisticResource> items = new ArrayList<>();

    @SerializedName("kind")
    private String kind = null;

    @SerializedName("metadata")
    private V1ListMeta metadata = null;

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @Override
    public List<V1OpportunisticResource> getItems() {
        return items;
    }

    public void setItems(List<V1OpportunisticResource> items) {
        this.items = items;
    }

    @Override
    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    @Override
    public V1ListMeta getMetadata() {
        return metadata;
    }

    public void setMetadata(V1ListMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        V1OpportunisticResourceList that = (V1OpportunisticResourceList) o;
        return Objects.equals(apiVersion, that.apiVersion) &&
                Objects.equals(items, that.items) &&
                Objects.equals(kind, that.kind) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiVersion, items, kind, metadata);
    }

    @Override
    public String toString() {
        return "V1OpportunisticResourceList{" +
                "apiVersion='" + apiVersion + '\'' +
                ", items=" + items +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
