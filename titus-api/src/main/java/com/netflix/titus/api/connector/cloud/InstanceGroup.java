/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.api.connector.cloud;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InstanceGroup {

    private final String id;
    private final String launchConfigurationName;
    private final String instanceType;
    private final int min;
    private final int desired;
    private final int max;
    private final boolean isLaunchSuspended;
    private final boolean isTerminateSuspended;
    private final Map<String, String> attributes;
    private final List<String> instanceIds;

    public InstanceGroup(String id,
                         String launchConfigurationName,
                         String instanceType,
                         int min,
                         int desired,
                         int max,
                         boolean isLaunchSuspended,
                         boolean isTerminateSuspended,
                         Map<String, String> attributes,
                         List<String> instanceIds) {
        this.id = id;
        this.launchConfigurationName = launchConfigurationName;
        this.instanceType = instanceType;
        this.min = min;
        this.desired = desired;
        this.max = max;
        this.isLaunchSuspended = isLaunchSuspended;
        this.isTerminateSuspended = isTerminateSuspended;
        this.attributes = attributes;
        this.instanceIds = instanceIds;
    }

    public String getId() {
        return id;
    }

    public String getLaunchConfigurationName() {
        return launchConfigurationName;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public int getMin() {
        return min;
    }

    public int getDesired() {
        return desired;
    }

    public int getMax() {
        return max;
    }

    public boolean isLaunchSuspended() {
        return isLaunchSuspended;
    }

    public boolean isTerminateSuspended() {
        return isTerminateSuspended;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public List<String> getInstanceIds() {
        return instanceIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InstanceGroup that = (InstanceGroup) o;
        return min == that.min &&
                desired == that.desired &&
                max == that.max &&
                isLaunchSuspended == that.isLaunchSuspended &&
                isTerminateSuspended == that.isTerminateSuspended &&
                Objects.equals(id, that.id) &&
                Objects.equals(launchConfigurationName, that.launchConfigurationName) &&
                Objects.equals(instanceType, that.instanceType) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(instanceIds, that.instanceIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, launchConfigurationName, instanceType, min, desired, max, isLaunchSuspended, isTerminateSuspended, attributes, instanceIds);
    }

    @Override
    public String toString() {
        return "InstanceGroup{" +
                "id='" + id + '\'' +
                ", launchConfigurationName='" + launchConfigurationName + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", min=" + min +
                ", desired=" + desired +
                ", max=" + max +
                ", isLaunchSuspended=" + isLaunchSuspended +
                ", isTerminateSuspended=" + isTerminateSuspended +
                ", attributes=" + attributes +
                ", instanceIds=" + instanceIds +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withId(id)
                .withLaunchConfigurationName(launchConfigurationName)
                .withInstanceType(instanceType)
                .withMin(min)
                .withDesired(desired)
                .withMax(max)
                .withIsLaunchSuspended(isLaunchSuspended)
                .withIsTerminateSuspended(isTerminateSuspended)
                .withAttributes(attributes)
                .withInstanceIds(instanceIds);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String launchConfigurationName;
        private String instanceType;
        private int min;
        private int desired;
        private int max;
        private boolean isLaunchSuspended;
        private boolean isTerminateSuspended;
        private List<String> instanceIds;
        private Map<String, String> attributes;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withLaunchConfigurationName(String launchConfigurationName) {
            this.launchConfigurationName = launchConfigurationName;
            return this;
        }

        public Builder withInstanceType(String instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder withMin(int min) {
            this.min = min;
            return this;
        }

        public Builder withDesired(int desired) {
            this.desired = desired;
            return this;
        }

        public Builder withMax(int max) {
            this.max = max;
            return this;
        }

        public Builder withIsLaunchSuspended(boolean isLaunchSuspended) {
            this.isLaunchSuspended = isLaunchSuspended;
            return this;
        }

        public Builder withIsTerminateSuspended(boolean isTerminateSuspended) {
            this.isTerminateSuspended = isTerminateSuspended;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withInstanceIds(List<String> instanceIds) {
            this.instanceIds = instanceIds;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withId(id)
                    .withLaunchConfigurationName(launchConfigurationName)
                    .withInstanceType(instanceType)
                    .withMin(min)
                    .withDesired(desired)
                    .withMax(max)
                    .withIsLaunchSuspended(isLaunchSuspended)
                    .withIsTerminateSuspended(isTerminateSuspended)
                    .withAttributes(attributes)
                    .withInstanceIds(instanceIds);
        }

        public InstanceGroup build() {
            return new InstanceGroup(id, launchConfigurationName, instanceType, min, desired, max,
                    isLaunchSuspended, isTerminateSuspended, attributes, instanceIds);
        }
    }
}
