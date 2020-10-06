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

import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;
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
}
