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

package com.netflix.titus.api.jobmanager.model.job;

import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;


@ClassInvariant.List({
        @ClassInvariant(condition = "min <= desired", message = "'min'(#{min}) must be <= 'desired'(#{desired})"),
        @ClassInvariant(condition = "desired <= max", message = "'desired'(#{desired}) must be <= 'max'(#{max})")
})
public class Capacity {

    @Min(value = 0, message = "'min' must be >= 0, but is #{#root}")
    private final int min;

    @Min(value = 0, message = "'desired' must be >= 0, but is #{#root}")
    private final int desired;

    @Min(value = 0, message = "'max' must be >= 0, but is #{#root}")
    private final int max;

    public Capacity(int min, int desired, int max) {
        this.min = min;
        this.desired = desired;
        this.max = max;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Capacity capacity = (Capacity) o;

        if (min != capacity.min) {
            return false;
        }
        if (desired != capacity.desired) {
            return false;
        }
        return max == capacity.max;
    }

    @Override
    public int hashCode() {
        int result = min;
        result = 31 * result + desired;
        result = 31 * result + max;
        return result;
    }

    @Override
    public String toString() {
        return "Capacity{" +
                "min=" + min +
                ", desired=" + desired +
                ", max=" + max +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Capacity.Builder newBuilder() {
        return new Capacity.Builder();
    }

    public static Capacity.Builder newBuilder(Capacity capacity) {
        return new Capacity.Builder()
                .withMin(capacity.getMin())
                .withDesired(capacity.getDesired())
                .withMax(capacity.getMax());
    }

    public static final class Builder {
        private int min = Integer.MIN_VALUE;
        private int desired;
        private int max = Integer.MIN_VALUE;

        private Builder() {
        }

        public Capacity.Builder withMin(int min) {
            this.min = min;
            return this;
        }

        public Capacity.Builder withDesired(int desired) {
            this.desired = desired;
            return this;
        }

        public Capacity.Builder withMax(int max) {
            this.max = max;
            return this;
        }

        public Capacity.Builder but() {
            return newBuilder().withMin(min).withDesired(desired).withMax(max);
        }

        public Capacity build() {
            return new Capacity(min, desired, max);
        }
    }
}
