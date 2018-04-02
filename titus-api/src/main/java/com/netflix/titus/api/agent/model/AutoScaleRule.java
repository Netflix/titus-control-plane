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

import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

@ClassInvariant.List({
        @ClassInvariant(condition = "min <= max", message = "'min'(#{min}) must be <= 'max'(#{max})"),
        @ClassInvariant(condition = "minIdleToKeep <= maxIdleToKeep", message = "'minIdleToKeep'(#{minIdleToKeep}) must be <= 'maxIdleToKeep'(#{maxIdleToKeep})"),
})
@ClassFieldsNotNull
public class AutoScaleRule {

    @Min(value = 0, message = "'min' must be >= 0, but is #{#root}")
    private final int min;

    @Min(value = 0, message = "'max' must be >= 0, but is #{#root}")
    private final int max;

    @Min(value = 0, message = "'minIdleToKeep' must be >= 0, but is #{#root}")
    private final int minIdleToKeep;

    @Min(value = 0, message = "'maxIdleToKeep' must be >= 0, but is #{#root}")
    private final int maxIdleToKeep;

    @Min(value = 0, message = "'coolDownSec' must be >= 0, but is #{#root}")
    private final int coolDownSec;

    @Min(value = 0, message = "'priority' must be >= 0, but is #{#root}")
    private final int priority;

    @Min(value = 1, message = "'shortfallAdjustingFactor' must be > 0, but is #{#root}")
    private final int shortfallAdjustingFactor;

    public AutoScaleRule(int min,
                         int max,
                         int minIdleToKeep,
                         int maxIdleToKeep,
                         int coolDownSec,
                         int priority,
                         int shortfallAdjustingFactor) {
        this.min = min;
        this.max = max;
        this.minIdleToKeep = minIdleToKeep;
        this.maxIdleToKeep = maxIdleToKeep;
        this.coolDownSec = coolDownSec;
        this.priority = priority;
        this.shortfallAdjustingFactor = shortfallAdjustingFactor;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public int getMinIdleToKeep() {
        return minIdleToKeep;
    }

    public int getMaxIdleToKeep() {
        return maxIdleToKeep;
    }

    public int getCoolDownSec() {
        return coolDownSec;
    }

    public int getPriority() {
        return priority;
    }

    public int getShortfallAdjustingFactor() {
        return shortfallAdjustingFactor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AutoScaleRule that = (AutoScaleRule) o;

        if (min != that.min) {
            return false;
        }
        if (max != that.max) {
            return false;
        }
        if (minIdleToKeep != that.minIdleToKeep) {
            return false;
        }
        if (maxIdleToKeep != that.maxIdleToKeep) {
            return false;
        }
        if (coolDownSec != that.coolDownSec) {
            return false;
        }
        if (priority != that.priority) {
            return false;
        }
        return shortfallAdjustingFactor == that.shortfallAdjustingFactor;
    }

    @Override
    public int hashCode() {
        int result = min;
        result = 31 * result + max;
        result = 31 * result + minIdleToKeep;
        result = 31 * result + maxIdleToKeep;
        result = 31 * result + coolDownSec;
        result = 31 * result + priority;
        result = 31 * result + shortfallAdjustingFactor;
        return result;
    }

    @Override
    public String toString() {
        return "AutoScaleRule{" +
                "min=" + min +
                ", max=" + max +
                ", minIdleToKeep=" + minIdleToKeep +
                ", maxIdleToKeep=" + maxIdleToKeep +
                ", coolDownSec=" + coolDownSec +
                ", priority=" + priority +
                ", shortfallAdjustingFactor=" + shortfallAdjustingFactor +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withMin(min)
                .withMax(max)
                .withMinIdleToKeep(minIdleToKeep)
                .withMaxIdleToKeep(maxIdleToKeep)
                .withCoolDownSec(coolDownSec)
                .withPriority(priority)
                .withShortfallAdjustingFactor(shortfallAdjustingFactor);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private int min;
        private int max;
        private int minIdleToKeep;
        private int maxIdleToKeep;
        private int coolDownSec;
        private int priority;
        private int shortfallAdjustingFactor;

        private Builder() {
        }

        public Builder withMin(int min) {
            this.min = min;
            return this;
        }

        public Builder withMax(int max) {
            this.max = max;
            return this;
        }

        public Builder withMinIdleToKeep(int minIdleToKeep) {
            this.minIdleToKeep = minIdleToKeep;
            return this;
        }

        public Builder withMaxIdleToKeep(int maxIdleToKeep) {
            this.maxIdleToKeep = maxIdleToKeep;
            return this;
        }

        public Builder withCoolDownSec(int coolDownSec) {
            this.coolDownSec = coolDownSec;
            return this;
        }

        public Builder withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder withShortfallAdjustingFactor(int shortfallAdjustingFactor) {
            this.shortfallAdjustingFactor = shortfallAdjustingFactor;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withMin(min)
                    .withMax(max)
                    .withMinIdleToKeep(minIdleToKeep)
                    .withMaxIdleToKeep(maxIdleToKeep)
                    .withCoolDownSec(coolDownSec)
                    .withPriority(priority)
                    .withShortfallAdjustingFactor(shortfallAdjustingFactor);
        }

        public AutoScaleRule build() {
            return new AutoScaleRule(min, max, minIdleToKeep, maxIdleToKeep, coolDownSec, priority, shortfallAdjustingFactor);
        }
    }
}
