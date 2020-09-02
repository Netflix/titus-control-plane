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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.supplementary.relocation.connector.Node;
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

    private static final Node ACTIVE_NODE = Node.newBuilder().withId("node1").withServerGroupId("serverGroup1").build();

    @Test
    public void testJobRelocationRequiredByPredicates() {
        Job<?> taggedJob = appendJobDescriptorAttribute(JOB,
                RelocationAttributes.RELOCATION_REQUIRED_BY, TASK.getStatus().getTimestamp() + 1
        );
        assertThat(checkIfNeedsRelocationPlan(taggedJob, TASK, ACTIVE_NODE)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(taggedJob, TASK, ACTIVE_NODE)).isEmpty();
    }

    @Test
    public void testJobRelocationRequiredByImmediatePredicates() {
        Job<?> taggedJob = appendJobDescriptorAttribute(JOB,
                RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY, TASK.getStatus().getTimestamp() + 1
        );
        assertThat(checkIfNeedsRelocationPlan(taggedJob, TASK, ACTIVE_NODE)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(taggedJob, TASK, ACTIVE_NODE)).isPresent();
    }

    @Test
    public void testTaskRelocationRequiredPredicates() {
        Task taggedTask = appendTaskAttribute(TASK, RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, taggedTask, ACTIVE_NODE)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, taggedTask, ACTIVE_NODE)).isEmpty();
    }

    @Test
    public void testTaskRelocationRequiredImmediatelyPredicates() {
        Task taggedTask = appendTaskAttribute(TASK, RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "true");
        assertThat(checkIfNeedsRelocationPlan(JOB, taggedTask, ACTIVE_NODE)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, taggedTask, ACTIVE_NODE)).isPresent();
    }

    @Test
    public void testInstanceRelocationRequiredPredicates() {
        Node decommissionedNode = ACTIVE_NODE.toBuilder().withRelocationRequired(true).build();
        assertThat(checkIfNeedsRelocationPlan(JOB, TASK, decommissionedNode)).isPresent();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, TASK, decommissionedNode)).isEmpty();
    }

    @Test
    public void testInstanceRelocationRequiredImmediatelyPredicates() {
        Node decommissionedNode = ACTIVE_NODE.toBuilder().withRelocationRequiredImmediately(true).build();
        assertThat(checkIfNeedsRelocationPlan(JOB, TASK, decommissionedNode)).isEmpty();
        assertThat(checkIfMustBeRelocatedImmediately(JOB, TASK, decommissionedNode)).isPresent();
    }
}