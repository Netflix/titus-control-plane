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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TaskRelocationPlanPredicateTest {

    private static final TaskRelocationPlan REFERENCE_PLAN = TaskRelocationPlan.newBuilder()
            .withTaskId("task1")
            .withReason(TaskRelocationReason.TaskMigration)
            .withReasonMessage("reason message")
            .withRelocationTime(123)
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final Job<BatchJobExt> job = JobGenerator.oneBatchJob();
    private final Task task = jobComponentStub.createJobAndTasks(job).getRight().get(0);
    private final TaskRelocationPlan plan = REFERENCE_PLAN.toBuilder().withTaskId(task.getId()).build();

    @Test
    public void testJobIdsFilter() {
        assertThat(newPredicte("jobIds", "").test(plan)).isTrue();
        assertThat(newPredicte("jobIds", "jobX," + job.getId()).test(plan)).isTrue();
        assertThat(newPredicte("jobIds", "jobX,jobY").test(plan)).isFalse();
    }

    @Test
    public void testTaskIdsFilter() {
        assertThat(newPredicte("taskIds", "").test(plan)).isTrue();
        assertThat(newPredicte("taskIds", "taskX," + task.getId()).test(plan)).isTrue();
        assertThat(newPredicte("taskIds", "taskX,taskY").test(plan)).isFalse();
    }

    @Test
    public void testApplicationFilter() {
        assertThat(newPredicte("applicationName", "").test(plan)).isTrue();
        assertThat(newPredicte("applicationName", job.getJobDescriptor().getApplicationName()).test(plan)).isTrue();
        assertThat(newPredicte("applicationName", "some" + task.getId()).test(plan)).isFalse();
    }

    @Test
    public void testCapacityGroupFilter() {
        assertThat(newPredicte("capacityGroup", "").test(plan)).isTrue();
        assertThat(newPredicte("capacityGroup", job.getJobDescriptor().getCapacityGroup()).test(plan)).isTrue();
        assertThat(newPredicte("capacityGroup", "some" + task.getId()).test(plan)).isFalse();
    }

    @Test
    public void testMixed() {
        Predicate<TaskRelocationPlan> predicate = newPredicte(
                "jobIds", job.getId(),
                "taskIds", task.getId(),
                "applicationName", job.getJobDescriptor().getApplicationName(),
                "capacityGroup", job.getJobDescriptor().getCapacityGroup()
        );

        assertThat(predicate.test(plan)).isTrue();
        assertThat(predicate.test(plan.toBuilder().withTaskId("taskX").build())).isFalse();
    }

    private Predicate<TaskRelocationPlan> newPredicte(String... criteria) {
        return new TaskRelocationPlanPredicate(
                jobComponentStub.getJobOperations(),
                TaskRelocationQuery.newBuilder()
                        .putAllFilteringCriteria(CollectionsExt.asMap(criteria))
                        .build()
        );
    }
}