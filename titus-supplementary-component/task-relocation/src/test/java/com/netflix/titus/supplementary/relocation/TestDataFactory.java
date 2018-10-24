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

package com.netflix.titus.supplementary.relocation;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.model.job.JobGenerator;

import static com.netflix.titus.api.agent.model.AgentFunctions.withId;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofBatchSize;
import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroups;
import static com.netflix.titus.testkit.model.agent.AgentTestFunctions.inState;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

public class TestDataFactory {

    public static final String ACTIVE_INSTANCE_GROUP = "active1";
    public static final String REMOVABLE_INSTANCE_GROUP = "removable1";

    public static DisruptionBudget newSelfManagedDisruptionBudget(long relocationTimeMs) {
        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                        .withRelocationTimeMs(relocationTimeMs)
                        .build()
                )
                .build();
    }

    public static DisruptionBudget newRelocationLimitDisruptionBudget(int limit) {
        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(RelocationLimitDisruptionBudgetPolicy.newBuilder()
                        .withLimit(limit)
                        .build()
                )
                .build();
    }

    public static Job<BatchJobExt> newBatchJob(String jobId, int size, DisruptionBudget disruptionBudget) {
        return JobGenerator.batchJobs(oneTaskBatchJobDescriptor().toBuilder()
                .withDisruptionBudget(disruptionBudget)
                .build()
                .but(ofBatchSize(size))
        ).getValue().but(JobFunctions.withJobId(jobId));
    }

    public static RelocationConnectorStubs activeRemovableSetup() {
        return activeRemovableSetup(TitusRuntimes.test());
    }

    public static RelocationConnectorStubs activeRemovableSetup(TitusRuntime titusRuntime) {
        MutableDataGenerator<AgentInstanceGroup> flexInstanceGroupGenerator = new MutableDataGenerator<>(agentServerGroups(Tier.Flex, 10));

        return new RelocationConnectorStubs(titusRuntime)
                .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId(ACTIVE_INSTANCE_GROUP), inState(InstanceGroupLifecycleState.Active)))
                .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId(REMOVABLE_INSTANCE_GROUP), inState(InstanceGroupLifecycleState.Removable)));
    }
}
