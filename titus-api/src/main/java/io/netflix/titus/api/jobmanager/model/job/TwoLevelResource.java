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

package io.netflix.titus.api.jobmanager.model.job;

import javax.validation.constraints.Min;

import io.netflix.titus.common.model.sanitizer.NeverNull;

/**
 *
 */
@NeverNull
public class TwoLevelResource {

    private final String name;

    private final String value;

    @Min(0)
    private final int index;

    private TwoLevelResource(String name, String value, int index) {
        this.name = name;
        this.value = value;
        this.index = index;
    }

    /**
     * Resource name (for example 'ENIs').
     */
    public String getName() {
        return name;
    }

    /**
     * Value associated with a resource at a given index position. For example, on a VM with 8 ENIs,
     * a task can be allocated ENI 5, and attach to it value "sg-e3e51a99:sg-f0f19494:sg-f2f19496", which are
     * task's security groups.
     */
    public String getValue() {
        return value;
    }

    /**
     * Index of a sub-resource allocated to a given task.
     */
    public int getIndex() {
        return index;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private String name;
        private String value;
        private int index;

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public Builder withIndex(int index) {
            this.index = index;
            return this;
        }

        public Builder but() {
            return newBuilder().withName(name).withValue(value).withIndex(index);
        }

        public TwoLevelResource build() {
            TwoLevelResource twoLevelResource = new TwoLevelResource(name, value, index);
            return twoLevelResource;
        }
    }
}
