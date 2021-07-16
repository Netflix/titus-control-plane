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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobFunctionsTest {

    private static final Task REFERENCE_TASK = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue()).getValue();

    private final Clock clock = Clocks.system();

    @Test
    public void testGetTimeInStateForCompletedTask() {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.Finished).withTimestamp(5000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withTimestamp(200).build(),
                        TaskStatus.newBuilder().withState(TaskState.Started).withTimestamp(1000).build(),
                        TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(2000).build()
                )
                .build();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated, clock)).contains(800L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started, clock)).contains(1000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated, clock)).contains(3000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished, clock).get()).isGreaterThan(0);
    }

    @Test
    public void testGetTimeInStateForFailedTask() {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(1000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build()
                )
                .build();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched, clock)).contains(900L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated, clock)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started, clock)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated, clock).get()).isGreaterThan(0);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished, clock)).isEmpty();
    }

    @Test
    public void testHasTransition() {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(1000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withReasonCode("step1").withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withReasonCode("step2").withTimestamp(100).build()
                )
                .build();
        assertThat(JobFunctions.containsExactlyTaskStates(task, TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated, TaskState.KillInitiated)).isTrue();
    }

    @Test
    public void testFindHardConstraint() {
        Job<BatchJobExt> job = JobFunctions.appendHardConstraint(JobGenerator.oneBatchJob(), "MyConstraint", "good");
        assertThat(JobFunctions.findHardConstraint(job, "myConstraint")).contains("good");
        assertThat(JobFunctions.findSoftConstraint(job, "myConstraint")).isEmpty();
    }

    @Test
    public void testFindSoftConstraint() {
        Job<BatchJobExt> job = JobFunctions.appendSoftConstraint(JobGenerator.oneBatchJob(), "MyConstraint", "good");
        assertThat(JobFunctions.findHardConstraint(job, "myConstraint")).isEmpty();
        assertThat(JobFunctions.findSoftConstraint(job, "myConstraint")).contains("good");
    }

    @Test
    public void testGroupTasksByResubmitOrder() {
        List<Task> group1 = taskWithReplacements(3);
        List<Task> group2 = taskWithReplacements(5);
        Task badOne = JobGenerator.oneBatchTask().toBuilder().withOriginalId("missing").build();
        List<Task> mixed = CollectionsExt.merge(group1, group2, Collections.singletonList(badOne));
        Collections.shuffle(mixed);

        Pair<Map<String, List<Task>>, List<Task>> result = JobFunctions.groupTasksByResubmitOrder(mixed);
        Map<String, List<Task>> ordered = result.getLeft();
        List<Task> rejected = result.getRight();

        // Ordered
        assertThat(ordered).hasSize(2);
        List<Task> firstOrder = ordered.get(group1.get(0).getId());
        assertThat(firstOrder).isEqualTo(group1);
        List<Task> secondOrder = ordered.get(group2.get(0).getId());
        assertThat(secondOrder).isEqualTo(group2);

        // Rejected
        assertThat(rejected).hasSize(1);
        assertThat(rejected.get(0).getId()).isEqualTo(badOne.getId());
    }

    private static List<Task> taskWithReplacements(int size) {
        BatchJobTask first = JobGenerator.oneBatchTask();
        List<Task> result = new ArrayList<>();
        result.add(first);
        Task last = first;
        for (int i = 1; i < size; i++) {
            BatchJobTask next = JobGenerator.oneBatchTask().toBuilder()
                    .withJobId(first.getJobId())
                    .withOriginalId(first.getId())
                    .withResubmitOf(last.getId())
                    .build();
            result.add(next);
            last = next;
        }
        return result;
    }
}
