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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.netflix.titus.common.util.NumberSequence;

class RuleSelector {

    private final NumberSequence slots;
    private final NumberSequence resubmits;
    private final Set<String> instanceIds;

    RuleSelector(NumberSequence slots, NumberSequence resubmits, Set<String> instanceIds) {
        this.slots = slots;
        this.resubmits = resubmits;
        this.instanceIds = instanceIds;
    }

    public NumberSequence getSlots() {
        return slots;
    }

    public NumberSequence getResubmits() {
        return resubmits;
    }

    public Set<String> getInstanceIds() {
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
        RuleSelector that = (RuleSelector) o;
        return Objects.equals(slots, that.slots) &&
                Objects.equals(resubmits, that.resubmits) &&
                Objects.equals(instanceIds, that.instanceIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slots, resubmits, instanceIds);
    }

    @Override
    public String toString() {
        return "RuleSelector{" +
                "slots=" + slots +
                ", resubmits=" + resubmits +
                ", instanceIds=" + instanceIds +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static RuleSelector everything() {
        return RuleSelector.newBuilder().build();
    }

    public static final class Builder {
        private NumberSequence slots;
        private int slotStep = 1;

        private NumberSequence resubmits;
        private int resubmitStep = 1;

        private Set<String> instanceIds = new HashSet<>();

        private Builder() {
        }

        public Builder withSlots(NumberSequence slots) {
            this.slots = slots;
            return this;
        }

        public Builder withSlotStep(int slotStep) {
            this.slotStep = slotStep;
            return this;
        }

        public Builder withResubmits(NumberSequence resubmits) {
            this.resubmits = resubmits;
            return this;
        }

        public Builder withResubmitStep(int resubmitStep) {
            this.resubmitStep = resubmitStep;
            return this;
        }

        public Builder withInstances(String... instanceIds) {
            for (String instanceId : instanceIds) {
                this.instanceIds.add(instanceId);
            }
            return this;
        }

        public RuleSelector build() {
            if (slots == null) {
                slots = NumberSequence.from(0);
            }
            if (resubmits == null) {
                resubmits = NumberSequence.from(0);
            }
            return new RuleSelector(slots.step(slotStep), resubmits.step(resubmitStep), instanceIds);
        }
    }
}
