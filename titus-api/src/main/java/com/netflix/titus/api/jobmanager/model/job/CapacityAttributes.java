package com.netflix.titus.api.jobmanager.model.job;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.util.Evaluators;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

@ClassInvariant.List({
        @ClassInvariant(condition = "!min.isPresent() || min.get() >=0", message = "'min'(#{min}) must be >= 0"),
        @ClassInvariant(condition = "!max.isPresent() || max.get() >=0", message = "'max'(#{max}) must be >= 0"),
        @ClassInvariant(condition = "!desired.isPresent() || desired.get() >=0", message = "'desired'(#{desired}) must be >= 0")
})
public class CapacityAttributes {
    private final Optional<Integer> min;
    private final Optional<Integer> desired;
    private final Optional<Integer> max;

    public CapacityAttributes(Optional<Integer> min, Optional<Integer> desired, Optional<Integer> max) {
        this.min = min;
        this.desired = desired;
        this.max = max;
    }

    public Optional<Integer> getMin() {
        return min;
    }

    public Optional<Integer> getDesired() {
        return desired;
    }

    public Optional<Integer> getMax() {
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
        CapacityAttributes that = (CapacityAttributes) o;
        return Objects.equals(min, that.min) &&
                Objects.equals(desired, that.desired) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, desired, max);
    }

    @Override
    public String toString() {
        return "CapacityAttributes{" +
                "min=" + min +
                ", desired=" + desired +
                ", max=" + max +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static CapacityAttributes.Builder newBuilder() {
        return new CapacityAttributes.Builder();
    }

    public static Builder newBuilder(CapacityAttributes capacityAttributes) {
        Builder builder = new Builder();
        builder.min = capacityAttributes.getMin();
        builder.max = capacityAttributes.getMax();
        builder.desired = capacityAttributes.getDesired();
        return builder;
    }

    public static final class Builder {
        private Optional<Integer> min = Optional.empty();
        private Optional<Integer> desired = Optional.empty();
        private Optional<Integer> max = Optional.empty();

        private Builder() {
        }

        public CapacityAttributes.Builder withMin(int min) {
            this.min = Optional.of(min);
            return this;
        }

        public CapacityAttributes.Builder withDesired(int desired) {
            this.desired= Optional.of(desired);
            return this;
        }

        public CapacityAttributes.Builder withMax(int max) {
            this.max = Optional.of(max);
            return this;
        }

        public CapacityAttributes build() {
            return new CapacityAttributes(min, desired, max);
        }
    }
}
