/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.kube;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;

public class EmbeddedServerGroup {

    private final String name;
    private final String instanceType;
    private final Set<String> zones;
    private final ResourceDimension nodeResources;
    private final String resourcePool;
    private final int size;

    public EmbeddedServerGroup(String name,
                               String instanceType,
                               Set<String> zones,
                               ResourceDimension nodeResources,
                               String resourcePool,
                               int size) {
        this.name = name;
        this.instanceType = instanceType;
        this.zones = zones;
        this.nodeResources = nodeResources;
        this.resourcePool = resourcePool;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public ResourceDimension getNodeResources() {
        return nodeResources;
    }

    public String getResourcePool() {
        return resourcePool;
    }

    public int getSize() {
        return size;
    }

    public Set<String> getZones() {
        return zones;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbeddedServerGroup that = (EmbeddedServerGroup) o;
        return size == that.size && Objects.equals(name, that.name) && Objects.equals(instanceType, that.instanceType) && Objects.equals(nodeResources, that.nodeResources) && Objects.equals(resourcePool, that.resourcePool);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, instanceType, nodeResources, resourcePool, size);
    }

    @Override
    public String toString() {
        return "EmbeddedServerGroup{" +
                "name='" + name + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", nodeResources=" + nodeResources +
                ", resourcePool='" + resourcePool + '\'' +
                ", size=" + size +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String name;
        private String instanceType;
        private ResourceDimension nodeResources;
        private String resourcePool;
        private int size;
        private Set<String> zones;

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withInstanceType(String instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder withNodeResources(ResourceDimension nodeResources) {
            this.nodeResources = nodeResources;
            return this;
        }

        public Builder withResourcePool(String resourcePool) {
            this.resourcePool = resourcePool;
            return this;
        }

        public Builder withSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withZones(Set<String> zones) {
            this.zones = zones;
            return this;
        }

        public EmbeddedServerGroup build() {
            Preconditions.checkNotNull(name, "Name is null");
            Preconditions.checkNotNull(instanceType, "Instance type is null");
            if (zones == null) {
                zones = CollectionsExt.asSet("zoneA", "zoneB", "zoneC");
            }
            if (nodeResources == null) {
                AwsInstanceType awsInstanceType = AwsInstanceType.withName(instanceType);
                nodeResources = ResourceDimension.newBuilder()
                        .withCpus(awsInstanceType.getDescriptor().getvCPUs())
                        .withGpu(awsInstanceType.getDescriptor().getvGPUs())
                        .withMemoryMB(awsInstanceType.getDescriptor().getMemoryGB() * 1024L)
                        .withDiskMB(awsInstanceType.getDescriptor().getStorageGB() * 1024L)
                        .withNetworkMbs(awsInstanceType.getDescriptor().getNetworkMbs())
                        .build();
            }
            return new EmbeddedServerGroup(name, instanceType, zones, nodeResources, resourcePool, size);
        }
    }
}
