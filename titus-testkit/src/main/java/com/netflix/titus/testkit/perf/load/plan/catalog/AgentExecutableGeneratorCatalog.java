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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;

import static com.netflix.titus.testkit.perf.load.plan.catalog.AgentExecutionPlanCatalog.periodicallyRedeployedPartition;

public final class AgentExecutableGeneratorCatalog {

    private static final int PARTITION_MAX_SIZE = 100;
    private static final int LONG_RUNNING_SERVICES_PER_M4 = 32;

    private AgentExecutableGeneratorCatalog() {
    }

    /**
     * Agent setup counterpart for {@link JobExecutableGeneratorCatalog#perfLoad(double)}.
     */
    public static List<ExecutionPlan> perfLoad(int sizeFactor) {
        int remained = 200 * sizeFactor;

        List<ExecutionPlan> plans = new ArrayList<>();
        for (int partitionIdx = 1; remained > 0; partitionIdx++) {
            int partitionSize = Math.min(PARTITION_MAX_SIZE, remained);
            plans.add(periodicallyRedeployedPartition("perfCritical" + partitionIdx, Tier.Critical, AwsInstanceType.M4_16XLarge, 0, partitionSize / 2, partitionSize));
            plans.add(periodicallyRedeployedPartition("perfFlex" + partitionIdx, Tier.Flex, AwsInstanceType.R4_16XLarge, 0, partitionSize / 2, partitionSize));
            remained -= partitionSize;
        }
        return plans;
    }

    /**
     * Agent setup counterpart for {@link JobExecutableGeneratorCatalog#longRunningServicesLoad(String)}.
     */
    public static List<ExecutionPlan> longRunningLoad() {
        int totalTasks = 0;
        for (Pair<Integer, Integer> sizeAndCount : JobExecutableGeneratorCatalog.LONG_RUNNING_SIZES_AND_COUNTS) {
            totalTasks += sizeAndCount.getLeft() * sizeAndCount.getRight();
        }

        String groupId = UUID.randomUUID().toString();
        List<ExecutionPlan> plans = new ArrayList<>();
        int remained = (int) Math.ceil(((double) totalTasks) / LONG_RUNNING_SERVICES_PER_M4);
        for (int idx = 1; remained > 0; idx++) {
            int partitionSize = Math.min(PARTITION_MAX_SIZE, remained);
            plans.add(periodicallyRedeployedPartition("perfCritical_" + groupId + "_" + idx, Tier.Critical, AwsInstanceType.M4_16XLarge, 0, partitionSize, partitionSize));
            remained -= partitionSize;
        }
        return plans;
    }
}
