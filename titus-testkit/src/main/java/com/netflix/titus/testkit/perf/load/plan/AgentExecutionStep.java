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

package com.netflix.titus.testkit.perf.load.plan;

import java.util.Objects;

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;

public abstract class AgentExecutionStep extends ExecutionStep {

    public static final String NAME_CREATE_PARTITION = "createPartition";
    public static final String NAME_RESIZE_PARTITION = "resizePartition";
    public static final String NAME_CHANGE_PARTITION_TIER = "changePartitionTier";
    public static final String NAME_CHANGE_PARTITION_LIFECYCLE_STATE = "changePartitionLifecycleState";

    protected AgentExecutionStep(String name) {
        super(name);
    }

    public static class CreatePartitionStep extends AgentExecutionStep {

        private final String partitionName;
        private final AwsInstanceType awsInstanceType;
        private final int min;
        private final int desired;
        private final int max;

        public CreatePartitionStep(String partitionName, AwsInstanceType awsInstanceType, int min, int desired, int max) {
            super(NAME_CREATE_PARTITION);
            this.partitionName = partitionName;
            this.awsInstanceType = awsInstanceType;
            this.min = min;
            this.desired = desired;
            this.max = max;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public AwsInstanceType getAwsInstanceType() {
            return awsInstanceType;
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
            if (!super.equals(o)) {
                return false;
            }
            CreatePartitionStep that = (CreatePartitionStep) o;
            return min == that.min &&
                    desired == that.desired &&
                    max == that.max &&
                    Objects.equals(partitionName, that.partitionName) &&
                    awsInstanceType == that.awsInstanceType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), partitionName, awsInstanceType, min, desired, max);
        }

        @Override
        public String toString() {
            return "CreatePartitionStep{" +
                    "partitionName='" + partitionName + '\'' +
                    ", awsInstanceType=" + awsInstanceType +
                    ", min=" + min +
                    ", desired=" + desired +
                    ", max=" + max +
                    '}';
        }
    }

    public static class ResizePartitionStep extends AgentExecutionStep {

        private final String partitionName;
        private final int min;
        private final int desired;
        private final int max;

        public ResizePartitionStep(String partitionName, int min, int desired, int max) {
            super(NAME_RESIZE_PARTITION);
            this.partitionName = partitionName;
            this.min = min;
            this.desired = desired;
            this.max = max;
        }

        public String getPartitionName() {
            return partitionName;
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
            if (!super.equals(o)) {
                return false;
            }
            CreatePartitionStep that = (CreatePartitionStep) o;
            return min == that.min &&
                    desired == that.desired &&
                    max == that.max &&
                    Objects.equals(partitionName, that.partitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), partitionName, min, desired, max);
        }

        @Override
        public String toString() {
            return "ResizePartitionStep{" +
                    "partitionName='" + partitionName + '\'' +
                    ", min=" + min +
                    ", desired=" + desired +
                    ", max=" + max +
                    '}';
        }
    }


    public static class ChangeTierStep extends ExecutionStep {

        private final String partition;
        private final Tier tier;

        public ChangeTierStep(String partition, Tier tier) {
            super(NAME_CHANGE_PARTITION_TIER);
            this.partition = partition;
            this.tier = tier;
        }

        public String getPartition() {
            return partition;
        }

        public Tier getTier() {
            return tier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ChangeTierStep that = (ChangeTierStep) o;
            return Objects.equals(partition, that.partition) &&
                    tier == that.tier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), partition, tier);
        }

        @Override
        public String toString() {
            return "ChangeTierStep{" +
                    "partition='" + partition + '\'' +
                    ", tier=" + tier +
                    '}';
        }
    }

    public static class ChangeLifecycleStateStep extends ExecutionStep {

        private final String partition;
        private final InstanceGroupLifecycleState lifecycleState;

        public ChangeLifecycleStateStep(String partition, InstanceGroupLifecycleState lifecycleState) {
            super(NAME_CHANGE_PARTITION_LIFECYCLE_STATE);
            this.partition = partition;
            this.lifecycleState = lifecycleState;
        }

        public String getPartition() {
            return partition;
        }

        public InstanceGroupLifecycleState getLifecycleState() {
            return lifecycleState;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ChangeLifecycleStateStep that = (ChangeLifecycleStateStep) o;
            return Objects.equals(partition, that.partition) &&
                    lifecycleState == that.lifecycleState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), partition, lifecycleState);
        }

        @Override
        public String toString() {
            return "ChangeLifecycleStateStep{" +
                    "partition='" + partition + '\'' +
                    ", lifecycleState=" + lifecycleState +
                    '}';
        }
    }

    public static ExecutionStep createPartition(String partitionName, AwsInstanceType awsInstanceType, int min, int desired, int max) {
        return new CreatePartitionStep(partitionName, awsInstanceType, min, desired, max);
    }

    public static ExecutionStep resizePartition(String partitionName, int min, int desired, int max) {
        return new ResizePartitionStep(partitionName, min, desired, max);
    }

    public static ExecutionStep changeTier(String partition, Tier tier) {
        return new ChangeTierStep(partition, tier);
    }

    public static ExecutionStep changeLifecycleState(String partition, InstanceGroupLifecycleState lifecycleState) {
        return new ChangeLifecycleStateStep(partition, lifecycleState);
    }
}
