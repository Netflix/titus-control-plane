/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.Objects;

import com.google.common.base.Preconditions;

public class ResourcePoolAssignment {

    private final String resourcePoolName;
    private final String rule;

    public ResourcePoolAssignment(String resourcePoolName, String rule) {
        this.resourcePoolName = resourcePoolName;
        this.rule = rule;
    }

    public String getResourcePoolName() {
        return resourcePoolName;
    }

    public String getRule() {
        return rule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourcePoolAssignment that = (ResourcePoolAssignment) o;
        return Objects.equals(resourcePoolName, that.resourcePoolName) &&
                Objects.equals(rule, that.rule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourcePoolName, rule);
    }

    @Override
    public String toString() {
        return "ResourcePoolAssignment{" +
                "resourcePoolName='" + resourcePoolName + '\'' +
                ", rule='" + rule + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String resourcePoolName;
        private String rule;

        private Builder() {
        }

        public Builder withResourcePoolName(String resourcePoolName) {
            this.resourcePoolName = resourcePoolName;
            return this;
        }

        public Builder withRule(String rule) {
            this.rule = rule;
            return this;
        }

        public ResourcePoolAssignment build() {
            Preconditions.checkNotNull(resourcePoolName, "resource pool name is null");
            Preconditions.checkNotNull(rule, "rule is null");
            return new ResourcePoolAssignment(resourcePoolName, rule);
        }
    }
}
