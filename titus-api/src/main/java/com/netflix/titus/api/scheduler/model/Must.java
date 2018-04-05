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

package com.netflix.titus.api.scheduler.model;

import java.util.Objects;
import javax.validation.constraints.NotNull;

/**
 * Type of system selector that filters out placement candidates if the operator is false.
 */
public class Must {

    @NotNull
    private final Operator operator;

    public Must(Operator operator) {
        this.operator = operator;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Must must = (Must) o;
        return Objects.equals(operator, must.operator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator);
    }

    @Override
    public String toString() {
        return "Must{" +
                "operator=" + operator +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {
        private Operator operator;

        private Builder() {
        }

        public Builder withOperator(Operator operator) {
            this.operator = operator;
            return this;
        }

        public Builder but() {
            return newBuilder().withOperator(operator);
        }

        public Must build() {
            return new Must(operator);
        }
    }
}
