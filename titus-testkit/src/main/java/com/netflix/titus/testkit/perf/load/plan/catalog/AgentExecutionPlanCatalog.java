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

package com.netflix.titus.testkit.perf.load.plan.catalog;

import java.time.Duration;

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;

public final class AgentExecutionPlanCatalog {

    private AgentExecutionPlanCatalog() {
    }

    /**
     * Creates two partitions, where always one is active and the other one inactive or removable. Periodically swaps them.
     */
    public static ExecutionPlan periodicallyRedeployedPartition(String baseName, Tier tier, AwsInstanceType awsInstanceType, int min, int desired, int max) {
        String partition1 = baseName + "v1";
        String partition2 = baseName + "v2";

        return ExecutionPlan.agentExecutionPlan()
                // Create both partitions
                .createPartition(partition1, awsInstanceType, min, desired, max)
                .changeTier(partition1, tier)
                .createPartition(partition2, awsInstanceType, min, desired, max)
                .changeTier(partition2, tier)

                .label("nextCycle")
                // partition1=active, partition1=removable
                .changeLifecycleState(partition1, InstanceGroupLifecycleState.Active)
                .changeLifecycleState(partition2, InstanceGroupLifecycleState.Removable)
                .randomDelay(Duration.ofMinutes(15), Duration.ofMinutes(30))
                .resizePartition(partition2, 0, 0, 0) // To forcefully kill all tasks on this partition
                .resizePartition(partition2, min, desired, max)

                // partition1=removable, partition2=active
                .changeLifecycleState(partition1, InstanceGroupLifecycleState.Removable)
                .changeLifecycleState(partition2, InstanceGroupLifecycleState.Active)
                .randomDelay(Duration.ofMinutes(15), Duration.ofMinutes(30))
                .resizePartition(partition1, 0, 0, 0) // To forcefully kill all tasks on this partition
                .resizePartition(partition1, min, desired, max)

                .loop("nextCycle")
                .build();
    }
}
