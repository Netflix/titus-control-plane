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

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;

public class AgentExecutionPlanBuilder extends ExecutionPlanBuilder<AgentExecutionPlanBuilder> {

    public AgentExecutionPlanBuilder createPartition(String partitionName, AwsInstanceType awsInstanceType, int min, int desired, int max) {
        steps.add(AgentExecutionStep.createPartition(partitionName, awsInstanceType, min, desired, max));
        return this;
    }

    public AgentExecutionPlanBuilder resizePartition(String partition, int min, int desired, int max) {
        steps.add(AgentExecutionStep.resizePartition(partition, min, desired, max));
        return this;
    }

    public AgentExecutionPlanBuilder changeTier(String partition, Tier tier) {
        steps.add(AgentExecutionStep.changeTier(partition, tier));
        return this;
    }

    public AgentExecutionPlanBuilder changeLifecycleState(String partition, InstanceGroupLifecycleState lifecycleState) {
        steps.add(AgentExecutionStep.changeLifecycleState(partition, lifecycleState));
        return this;
    }

    @Override
    public ExecutionPlan build() {
        return new ExecutionPlan(totalRunningTime, steps);
    }
}
