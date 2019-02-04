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

package com.netflix.titus.supplementary.relocation.util;

import java.util.Collections;

import com.netflix.titus.api.agent.model.AgentFunctions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.supplementary.relocation.RelocationAttributes;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.appendJobDescriptorAttribute;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.appendTaskAttribute;
import static com.netflix.titus.supplementary.relocation.util.RelocationPredicates.checkIfMustBeRelocatedImmediately;
import static com.netflix.titus.supplementary.relocation.util.RelocationPredicates.checkIfNeedsRelocationPlan;
import static org.assertj.core.api.Assertions.assertThat;

public class RelocationPredicatesTest {

    private static final Job<?> JOB = JobFunctions.changeDisruptionBudget(
            JobGenerator.oneBatchJob(), DisruptionBudgetGenerator.budget(
                    DisruptionBudgetGenerator.selfManagedPolicy(1_000),
                    DisruptionBudgetGenerator.unlimitedRate(),
                    Collections.emptyList()
            ));

    private static final Task TASK = JobGenerator.oneBatchTask().toBuilder().withJobId(JOB.getId()).build();

    private static final AgentInstanceGroup INSTANCE_GROUP = AgentGenerator.agentServerGroups().getValue();

    private static final AgentInstance INSTANCE = AgentGenerator.agentInstances(INSTANCE_GROUP).getValue();

    @Test
    public void testJobRelocationRequiredByPredicates() {
        Job<?> taggedJob = appendJobDescriptorAttribute(JOB,
                RelocationAttributes.RELOCATION_REQUIRED_BY, TASK.getStatus().getTimestamp() + 1
        );
        assertThat(checkIfNeedsRelocationPlan(taggedJob, TASK, INSTANCE_GROUP, INSTANCE)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(taggedJob, TASK, INSTANCE)).isEmpty();
    }

    @Test
    public void testJobRelocationRequiredByImmediatePredicates() {
        Job<?> taggedJob = appendJobDescriptorAttribute(JOB,
                RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY, TASK.getStatus().getTimestamp() + 1
        );
        assertThat(checkIfNeedsRelocationPlan(taggedJob, TASK, INSTANCE_GROUP, INSTANCE)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(taggedJob, TASK, INSTANCE)).isPresent();
    }

    @Test
    public void testTaskRelocationRequiredPredicates() {
        Task taggedTask = appendTaskAttribute(TASK, RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, taggedTask, INSTANCE_GROUP, INSTANCE)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, taggedTask, INSTANCE)).isEmpty();
    }

    @Test
    public void testTaskRelocationRequiredImmediatelyPredicates() {
        Task taggedTask = appendTaskAttribute(TASK, RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, taggedTask, INSTANCE_GROUP, INSTANCE)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, taggedTask, INSTANCE)).isPresent();
    }

    @Test
    public void testInstanceRelocationRequiredPredicates() {
        AgentInstance taggedInstance = AgentFunctions.appendInstanceAttribute(INSTANCE, RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, TASK, INSTANCE_GROUP, taggedInstance)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, TASK, taggedInstance)).isEmpty();
    }

    @Test
    public void testInstanceRelocationRequiredImmediatelyPredicates() {
        AgentInstance taggedInstance = AgentFunctions.appendInstanceAttribute(INSTANCE, RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, TASK, INSTANCE_GROUP, taggedInstance)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, TASK, taggedInstance)).isPresent();
    }
}