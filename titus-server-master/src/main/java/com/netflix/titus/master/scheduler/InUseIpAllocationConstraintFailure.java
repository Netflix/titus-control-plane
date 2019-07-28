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

package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.model.Tier;

public class InUseIpAllocationConstraintFailure extends TaskPlacementFailure {
    private final String inUseTaskId;

    public InUseIpAllocationConstraintFailure(String taskId,
                                              Optional<String> inUseTaskIdOptional,
                                              int agentCount,
                                              Tier tier,
                                              Map<String, Object> rawData) {
        super(taskId, FailureKind.WaitingForInUseIpAllocation, agentCount, tier, rawData);
        this.inUseTaskId = inUseTaskIdOptional.orElse("unknown");
    }

    public String getInUseTaskId() {
        return inUseTaskId;
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
        InUseIpAllocationConstraintFailure that = (InUseIpAllocationConstraintFailure) o;
        return inUseTaskId.equals(that.inUseTaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inUseTaskId);
    }

    @Override
    public String toString() {
        return "InUseIpAllocationConstraintFailure{" +
                "inUseTaskId='" + inUseTaskId + '\'' +
                '}';
    }
}
