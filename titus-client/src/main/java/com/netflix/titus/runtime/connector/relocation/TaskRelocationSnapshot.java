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

package com.netflix.titus.runtime.connector.relocation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatedSnapshot;

public class TaskRelocationSnapshot extends ReplicatedSnapshot {

    private static final TaskRelocationSnapshot EMPTY = newBuilder().build();

    private final String snapshotId;
    private final Map<String, TaskRelocationPlan> plans;
    private final String summaryString;

    public TaskRelocationSnapshot(String snapshotId, Map<String, TaskRelocationPlan> plans) {
        this.snapshotId = snapshotId;
        this.plans = Collections.unmodifiableMap(plans);
        this.summaryString = computeSignature();
    }

    public static TaskRelocationSnapshot empty() {
        return EMPTY;
    }

    public Map<String, TaskRelocationPlan> getPlans() {
        return plans;
    }

    @Override
    public String toSummaryString() {
        return summaryString;
    }

    @Override
    public String toString() {
        return "TaskRelocationSnapshot{" +
                "plans=" + plans +
                '}';
    }

    private String computeSignature() {
        return "TaskRelocationSnapshot{snapshotId=" + snapshotId +
                ", plans=" + plans.size() +
                "}";
    }

    public Builder toBuilder() {
        return newBuilder().withPlans(plans);
    }

    public static Builder newBuilder() {
        return new Builder(UUID.randomUUID().toString());
    }

    public static final class Builder {
        private final String snapshotId;
        private Map<String, TaskRelocationPlan> plans;

        private Builder(String snapshotId) {
            this.snapshotId = snapshotId;
        }

        public Builder withPlans(Map<String, TaskRelocationPlan> plans) {
            this.plans = new HashMap<>(plans);
            return this;
        }

        public Builder addPlan(TaskRelocationPlan plan) {
            if (plans == null) {
                this.plans = new HashMap<>();
            }
            plans.put(plan.getTaskId(), plan);
            return this;
        }

        public Builder removePlan(String taskId) {
            if (plans != null) {
                plans.remove(taskId);
            }
            return this;
        }

        public TaskRelocationSnapshot build() {
            return new TaskRelocationSnapshot(snapshotId, Evaluators.getOrDefault(plans, Collections.emptyMap()));
        }
    }
}
