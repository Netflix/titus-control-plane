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

import java.util.Objects;

import com.netflix.titus.common.util.NumberSequence;

class ContainerSelector {

    private final NumberSequence slots;
    private final NumberSequence resubmits;

    ContainerSelector(NumberSequence slots, NumberSequence resubmits) {
        this.slots = slots;
        this.resubmits = resubmits;
    }

    public NumberSequence getSlots() {
        return slots;
    }

    public NumberSequence getResubmits() {
        return resubmits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerSelector selector = (ContainerSelector) o;
        return Objects.equals(slots, selector.slots) &&
                Objects.equals(resubmits, selector.resubmits);
    }

    @Override
    public int hashCode() {

        return Objects.hash(slots, resubmits);
    }

    @Override
    public String toString() {
        return "ContainerSelector{" +
                "slots=" + slots +
                ", resubmits=" + resubmits +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static ContainerSelector everything() {
        return ContainerSelector.newBuilder().build();
    }

    public static final class Builder {
        private NumberSequence slots;
        private int slotStep = 1;

        private NumberSequence resubmits;
        private int resubmitStep = 1;

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

        public ContainerSelector build() {
            if (slots == null) {
                slots = NumberSequence.from(0);
            }
            if (resubmits == null) {
                resubmits = NumberSequence.from(0);
            }
            return new ContainerSelector(slots.step(slotStep), resubmits.step(resubmitStep));
        }
    }
}
