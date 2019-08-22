/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Objects;

public class OpportunisticCpuAllocation {
    private final String taskId;
    private final String agentId;
    private final String allocationId;
    private final int cpuCount;

    public OpportunisticCpuAllocation(String taskId, String agentId, String allocationId, int cpuCount) {
        this.taskId = taskId;
        this.agentId = agentId;
        this.allocationId = allocationId;
        this.cpuCount = cpuCount;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public int getCpuCount() {
        return cpuCount;
    }

    @Override
    public String toString() {
        return "OpportunisticCpuAllocation{" +
                "taskId='" + taskId + '\'' +
                ", agentId='" + agentId + '\'' +
                ", allocationId='" + allocationId + '\'' +
                ", cpuCount=" + cpuCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OpportunisticCpuAllocation)) {
            return false;
        }
        OpportunisticCpuAllocation that = (OpportunisticCpuAllocation) o;
        return cpuCount == that.cpuCount &&
                taskId.equals(that.taskId) &&
                agentId.equals(that.agentId) &&
                allocationId.equals(that.allocationId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, agentId, allocationId, cpuCount);
    }

    public static final class Builder {
        private String taskId;
        private String agentId;
        private String allocationId;
        private int cpuCount;

        private Builder() {
        }

        public Builder withTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder withAgentId(String agentId) {
            this.agentId = agentId;
            return this;
        }

        public Builder withAllocationId(String allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public Builder withCpuCount(int cpuCount) {
            this.cpuCount = cpuCount;
            return this;
        }

        public OpportunisticCpuAllocation build() {
            return new OpportunisticCpuAllocation(taskId, agentId, allocationId, cpuCount);
        }
    }
}
