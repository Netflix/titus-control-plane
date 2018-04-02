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

package com.netflix.titus.api.agent.model;

import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.model.sanitizer.CollectionInvariants;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

@ClassInvariant.List({
        @ClassInvariant(condition = "min <= desired", message = "'min'(#{min}) must be <= 'desired'(#{desired})"),
        @ClassInvariant(condition = "desired <= max", message = "'desired'(#{desired}) must be <= 'max'(#{max})")
})
@ClassFieldsNotNull
public class AgentInstanceGroup {

    private final String id;

    private final String instanceType;

    private final Tier tier;

    @Valid
    private final ResourceDimension resourceDimension;

    @Min(value = 0, message = "'min' must be >= 0, but is #{#root}")
    private final int min;

    @Min(value = 0, message = "'desired' must be >= 0, but is #{#root}")
    private final int desired;

    @Min(value = 0, message = "'current' must be >= 0, but is #{#root}")
    private final int current;

    @Min(value = 0, message = "'max' must be >= 0, but is #{#root}")
    private final int max;

    private final boolean isLaunchEnabled;

    private final boolean isTerminateEnabled;

    private final AutoScaleRule autoScaleRule;

    private final InstanceGroupLifecycleStatus lifecycleStatus;

    @Min(value = 0, message = "Negative launch timestamp value")
    private final long launchTimestamp;

    @CollectionInvariants
    private final Map<String, String> attributes;

    public AgentInstanceGroup(String id,
                              String instanceType,
                              Tier tier,
                              ResourceDimension resourceDimension,
                              int min,
                              int desired,
                              int current,
                              int max,
                              boolean isLaunchEnabled,
                              boolean isTerminateEnabled,
                              AutoScaleRule autoScaleRule,
                              InstanceGroupLifecycleStatus lifecycleStatus,
                              long launchTimestamp,
                              Map<String, String> attributes) {
        this.id = id;
        this.instanceType = instanceType;
        this.tier = tier;
        this.resourceDimension = resourceDimension;
        this.min = min;
        this.desired = desired;
        this.current = current;
        this.max = max;
        this.isLaunchEnabled = isLaunchEnabled;
        this.isTerminateEnabled = isTerminateEnabled;
        this.autoScaleRule = autoScaleRule;
        this.lifecycleStatus = lifecycleStatus;
        this.launchTimestamp = launchTimestamp;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public Tier getTier() {
        return tier;
    }

    public ResourceDimension getResourceDimension() {
        return resourceDimension;
    }

    public int getMin() {
        return min;
    }

    public int getDesired() {
        return desired;
    }

    public int getCurrent() {
        return current;
    }

    public int getMax() {
        return max;
    }

    public boolean isLaunchEnabled() {
        return isLaunchEnabled;
    }

    public boolean isTerminateEnabled() {
        return isTerminateEnabled;
    }

    public AutoScaleRule getAutoScaleRule() {
        return autoScaleRule;
    }

    public InstanceGroupLifecycleStatus getLifecycleStatus() {
        return lifecycleStatus;
    }

    public long getLaunchTimestamp() {
        return launchTimestamp;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentInstanceGroup that = (AgentInstanceGroup) o;

        if (min != that.min) {
            return false;
        }
        if (desired != that.desired) {
            return false;
        }
        if (current != that.current) {
            return false;
        }
        if (max != that.max) {
            return false;
        }
        if (isLaunchEnabled != that.isLaunchEnabled) {
            return false;
        }
        if (isTerminateEnabled != that.isTerminateEnabled) {
            return false;
        }
        if (launchTimestamp != that.launchTimestamp) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (instanceType != null ? !instanceType.equals(that.instanceType) : that.instanceType != null) {
            return false;
        }
        if (tier != that.tier) {
            return false;
        }
        if (resourceDimension != null ? !resourceDimension.equals(that.resourceDimension) : that.resourceDimension != null) {
            return false;
        }
        if (autoScaleRule != null ? !autoScaleRule.equals(that.autoScaleRule) : that.autoScaleRule != null) {
            return false;
        }
        if (lifecycleStatus != null ? !lifecycleStatus.equals(that.lifecycleStatus) : that.lifecycleStatus != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (instanceType != null ? instanceType.hashCode() : 0);
        result = 31 * result + (tier != null ? tier.hashCode() : 0);
        result = 31 * result + (resourceDimension != null ? resourceDimension.hashCode() : 0);
        result = 31 * result + min;
        result = 31 * result + desired;
        result = 31 * result + current;
        result = 31 * result + max;
        result = 31 * result + (isLaunchEnabled ? 1 : 0);
        result = 31 * result + (isTerminateEnabled ? 1 : 0);
        result = 31 * result + (autoScaleRule != null ? autoScaleRule.hashCode() : 0);
        result = 31 * result + (lifecycleStatus != null ? lifecycleStatus.hashCode() : 0);
        result = 31 * result + (int) (launchTimestamp ^ (launchTimestamp >>> 32));
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentInstanceGroup{" +
                "id='" + id + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", tier=" + tier +
                ", resourceDimension=" + resourceDimension +
                ", min=" + min +
                ", desired=" + desired +
                ", current=" + current +
                ", max=" + max +
                ", isLaunchEnabled=" + isLaunchEnabled +
                ", isTerminateEnabled=" + isTerminateEnabled +
                ", autoScaleRule=" + autoScaleRule +
                ", lifecycleStatus=" + lifecycleStatus +
                ", launchTimestamp=" + launchTimestamp +
                ", attributes=" + attributes +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withId(id)
                .withInstanceType(instanceType)
                .withTier(tier)
                .withResourceDimension(resourceDimension)
                .withMin(min)
                .withDesired(desired)
                .withCurrent(current)
                .withMax(max)
                .withIsLaunchEnabled(isLaunchEnabled)
                .withIsTerminateEnabled(isTerminateEnabled)
                .withAutoScaleRule(autoScaleRule)
                .withLifecycleStatus(lifecycleStatus)
                .withLaunchTimestamp(launchTimestamp)
                .withAttributes(attributes);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String instanceType;
        private Tier tier;
        private ResourceDimension resourceDimension = ResourceDimension.empty();
        private int min;
        private int desired;
        private int current;
        private int max;
        private boolean isLaunchEnabled;
        private boolean isTerminateEnabled;
        private AutoScaleRule autoScaleRule;
        private InstanceGroupLifecycleStatus instanceGroupLifecycleStatus;
        private long launchTimestamp;
        private Map<String, String> attributes;
        private long timestamp;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withInstanceType(String instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder withTier(Tier tier) {
            this.tier = tier;
            return this;
        }

        public Builder withResourceDimension(ResourceDimension resourceDimension) {
            this.resourceDimension = resourceDimension;
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

        public Builder withCurrent(int current) {
            this.current = current;
            return this;
        }

        public Builder withMax(int max) {
            this.max = max;
            return this;
        }

        public Builder withIsLaunchEnabled(boolean isLaunchEnabled) {
            this.isLaunchEnabled = isLaunchEnabled;
            return this;
        }

        public Builder withIsTerminateEnabled(boolean isTerminateEnabled) {
            this.isTerminateEnabled = isTerminateEnabled;
            return this;
        }

        public Builder withAutoScaleRule(AutoScaleRule autoScaleRule) {
            this.autoScaleRule = autoScaleRule;
            return this;
        }

        public Builder withLifecycleStatus(InstanceGroupLifecycleStatus instanceGroupLifecycleStatus) {
            this.instanceGroupLifecycleStatus = instanceGroupLifecycleStatus;
            return this;
        }

        public Builder withLaunchTimestamp(long launchTimestamp) {
            this.launchTimestamp = launchTimestamp;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withId(id).withInstanceType(instanceType).withTier(tier).withResourceDimension(resourceDimension).withMin(min).withDesired(desired).withCurrent(current).withMax(max).withIsLaunchEnabled(isLaunchEnabled).withIsTerminateEnabled(isTerminateEnabled).withAutoScaleRule(autoScaleRule).withLifecycleStatus(instanceGroupLifecycleStatus).withLaunchTimestamp(launchTimestamp).withAttributes(attributes).withTimestamp(timestamp);
        }

        public AgentInstanceGroup build() {

            AgentInstanceGroup agentInstanceGroup = new AgentInstanceGroup(
                    id, instanceType, tier, resourceDimension, min, desired, current, max, isLaunchEnabled,
                    isTerminateEnabled, autoScaleRule, instanceGroupLifecycleStatus, launchTimestamp, attributes);
            return agentInstanceGroup;
        }
    }
}
