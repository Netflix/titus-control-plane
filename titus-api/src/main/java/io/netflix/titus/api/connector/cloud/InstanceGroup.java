/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.connector.cloud;

import java.util.List;
import java.util.Map;

public class InstanceGroup {

    private final String id;
    private final String launchConfigurationName;
    private final int min;
    private final int desired;
    private final int max;
    private final boolean isLaunchSuspended;
    private final boolean isTerminateSuspended;
    private final Map<String, String> attributes;
    private final List<String> instanceIds;

    public InstanceGroup(String id,
                         String launchConfigurationName,
                         int min,
                         int desired,
                         int max,
                         boolean isLaunchSuspended,
                         boolean isTerminateSuspended,
                         Map<String, String> attributes,
                         List<String> instanceIds) {
        this.id = id;
        this.launchConfigurationName = launchConfigurationName;
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

        if (min != that.min) {
            return false;
        }
        if (desired != that.desired) {
            return false;
        }
        if (max != that.max) {
            return false;
        }
        if (isLaunchSuspended != that.isLaunchSuspended) {
            return false;
        }
        if (isTerminateSuspended != that.isTerminateSuspended) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (launchConfigurationName != null ? !launchConfigurationName.equals(that.launchConfigurationName) : that.launchConfigurationName != null) {
            return false;
        }
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
            return false;
        }
        return instanceIds != null ? instanceIds.equals(that.instanceIds) : that.instanceIds == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (launchConfigurationName != null ? launchConfigurationName.hashCode() : 0);
        result = 31 * result + min;
        result = 31 * result + desired;
        result = 31 * result + max;
        result = 31 * result + (isLaunchSuspended ? 1 : 0);
        result = 31 * result + (isTerminateSuspended ? 1 : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (instanceIds != null ? instanceIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "InstanceGroup{" +
                "id='" + id + '\'' +
                ", launchConfigurationName='" + launchConfigurationName + '\'' +
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
                    .withMin(min)
                    .withDesired(desired)
                    .withMax(max)
                    .withIsLaunchSuspended(isLaunchSuspended)
                    .withIsTerminateSuspended(isTerminateSuspended)
                    .withAttributes(attributes)
                    .withInstanceIds(instanceIds);
        }

        public InstanceGroup build() {
            InstanceGroup instanceGroup = new InstanceGroup(id, launchConfigurationName, min, desired, max, isLaunchSuspended, isTerminateSuspended, attributes, instanceIds);
            return instanceGroup;
        }
    }
}
