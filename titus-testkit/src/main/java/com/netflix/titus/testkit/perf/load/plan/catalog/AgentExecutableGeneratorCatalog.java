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

import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;

import static com.netflix.titus.testkit.perf.load.plan.catalog.AgentExecutionPlanCatalog.periodicallyRedeployedPartition;

public final class AgentExecutableGeneratorCatalog {

    private static final int PARTITION_MAX_SIZE = 100;

    private AgentExecutableGeneratorCatalog() {
    }

    /**
     * Agent setup counterpart for {@link JobExecutableGeneratorCatalog#perfLoad(double)}.
     */
    public static List<ExecutionPlan> perfLoad(int sizeFactor) {
        int remained = 200 * sizeFactor;

        List<ExecutionPlan> plans = new ArrayList<>();
        int partitionIdx = 1;
        while (remained > 0) {
            int partitionSize = Math.min(PARTITION_MAX_SIZE, remained);
            plans.add(periodicallyRedeployedPartition("perfCritical" + partitionIdx, Tier.Critical, AwsInstanceType.M4_16XLarge, 0, partitionSize / 2, partitionSize));
            plans.add(periodicallyRedeployedPartition("perfFlex" + partitionIdx, Tier.Flex, AwsInstanceType.R4_16XLarge, 0, partitionSize / 2, partitionSize));
            remained -= partitionSize;
            partitionIdx++;
        }
        return plans;
    }
}
