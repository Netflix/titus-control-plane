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
import java.util.Objects;
import java.util.function.Function;
import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.model.sanitizer.CollectionInvariants;

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
        return min == that.min &&
                desired == that.desired &&
                current == that.current &&
                max == that.max &&
                isLaunchEnabled == that.isLaunchEnabled &&
                isTerminateEnabled == that.isTerminateEnabled &&
                launchTimestamp == that.launchTimestamp &&
                Objects.equals(id, that.id) &&
                Objects.equals(instanceType, that.instanceType) &&
                tier == that.tier &&
                Objects.equals(resourceDimension, that.resourceDimension) &&
                Objects.equals(lifecycleStatus, that.lifecycleStatus) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, instanceType, tier, resourceDimension, min, desired, current, max, isLaunchEnabled, isTerminateEnabled, lifecycleStatus, launchTimestamp, attributes);
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
                .withLifecycleStatus(lifecycleStatus)
                .withLaunchTimestamp(launchTimestamp)
                .withAttributes(attributes);
    }

    @SafeVarargs
    public final AgentInstanceGroup but(Function<AgentInstanceGroup, AgentInstanceGroup>... modifiers) {
        AgentInstanceGroup result = this;
        for (Function<AgentInstanceGroup, AgentInstanceGroup> modifier : modifiers) {
            result = modifier.apply(result);
        }
        return result;
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
            return newBuilder().withId(id).withInstanceType(instanceType).withTier(tier).withResourceDimension(resourceDimension).withMin(min).withDesired(desired).withCurrent(current).withMax(max).withIsLaunchEnabled(isLaunchEnabled).withIsTerminateEnabled(isTerminateEnabled).withLifecycleStatus(instanceGroupLifecycleStatus).withLaunchTimestamp(launchTimestamp).withAttributes(attributes).withTimestamp(timestamp);
        }

        public AgentInstanceGroup build() {

            AgentInstanceGroup agentInstanceGroup = new AgentInstanceGroup(
                    id, instanceType, tier, resourceDimension, min, desired, current, max, isLaunchEnabled,
                    isTerminateEnabled, instanceGroupLifecycleStatus, launchTimestamp, attributes);
            return agentInstanceGroup;
        }
    }
}
