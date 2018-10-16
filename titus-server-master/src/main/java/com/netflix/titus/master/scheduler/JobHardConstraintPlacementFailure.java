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

package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.netflix.titus.api.model.Tier;

public class JobHardConstraintPlacementFailure extends TaskPlacementFailure {

    private final Set<String> hardConstraints;

    public JobHardConstraintPlacementFailure(String taskId,
                                             int agentCount,
                                             Set<String> hardConstraints,
                                             Tier tier,
                                             Map<String, Object> rawData) {
        super(taskId, FailureKind.JobHardConstraint, agentCount, tier, rawData);
        this.hardConstraints = hardConstraints;
    }

    public Set<String> getHardConstraints() {
        return hardConstraints;
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
        JobHardConstraintPlacementFailure that = (JobHardConstraintPlacementFailure) o;
        return Objects.equals(hardConstraints, that.hardConstraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hardConstraints);
    }

    @Override
    public String toString() {
        return "JobHardConstraintPlacementFailure{" +
                "taskId='" + getTaskId() + '\'' +
                ", failureKind=" + getFailureKind() +
                ", tier=" + getTier() +
                ", agentCount=" + getAgentCount() +
                ", hardConstraints=" + hardConstraints +
                "}";
    }
}
