/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactories;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NeedsMigrationQueryHandlerTest {

    private static final JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> QUERY_ALL = JobQueryCriteria.<TaskStatus.TaskState, JobDescriptor.JobSpecCase>newBuilder()
            .withNeedsMigration(true)
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);

    private final RelocationDataReplicator relocationDataReplicator = mock(RelocationDataReplicator.class);

    private final NeedsMigrationQueryHandler migrationHandler = new NeedsMigrationQueryHandler(
            jobDataReplicator,
            relocationDataReplicator,
            EmptyLogStorageInfo.empty(),
            titusRuntime
    );

    @Before
    public void setUp() {
        when(jobDataReplicator.getCurrent()).thenReturn(JobSnapshotFactories.newDefaultEmptySnapshot(titusRuntime));
        when(relocationDataReplicator.getCurrent()).thenReturn(TaskRelocationSnapshot.newBuilder().build());
    }

    @Test
    public void testFindJobs() {
        Pair<Job<?>, List<Task>> job1 = addToJobDataReplicator(newJobAndTasks("job1", 2));
        Pair<Job<?>, List<Task>> job2 = addToJobDataReplicator(newJobAndTasks("job2", 4));
        Pair<Job<?>, List<Task>> job3 = addToJobDataReplicator(newJobAndTasks("job3", 0));

        // No migration required yet
        PageResult<com.netflix.titus.grpc.protogen.Job> result = migrationHandler.findJobs(QUERY_ALL, Page.first(10));
        assertThat(result.getItems()).isEmpty();

        // Enable migration
        setNeedsRelocation(job1, 2);
        setNeedsRelocation(job2, 1);

        // Limit query to job1
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> query = JobQueryCriteria.<TaskStatus.TaskState, JobDescriptor.JobSpecCase>newBuilder()
                .withNeedsMigration(true)
                .withJobIds(CollectionsExt.asSet("job1"))
                .build();
        PageResult<com.netflix.titus.grpc.protogen.Job> job1TasksOnly = migrationHandler.findJobs(query, Page.first(10));
        assertThat(job1TasksOnly.getItems()).hasSize(1);
        assertThat(job1TasksOnly.getItems().get(0).getId()).isEqualTo("job1");

        // Now everything
        PageResult<com.netflix.titus.grpc.protogen.Job> allJobs = migrationHandler.findJobs(QUERY_ALL, Page.first(10));
        assertThat(allJobs.getItems()).hasSize(2);
    }

    @Test
    public void testFindTasks() {
        Pair<Job<?>, List<Task>> job1 = addToJobDataReplicator(newJobAndTasks("job1", 2));
        Pair<Job<?>, List<Task>> job2 = addToJobDataReplicator(newJobAndTasks("job2", 4));

        // No migration required yet
        PageResult<com.netflix.titus.grpc.protogen.Task> result = migrationHandler.findTasks(QUERY_ALL, Page.first(10));
        assertThat(result.getItems()).isEmpty();

        // Enable migration
        setNeedsRelocation(job1, 2);
        setNeedsRelocation(job2, 1);

        // Limit query to job1
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> query = JobQueryCriteria.<TaskStatus.TaskState, JobDescriptor.JobSpecCase>newBuilder()
                .withNeedsMigration(true)
                .withJobIds(CollectionsExt.asSet("job1"))
                .build();
        PageResult<com.netflix.titus.grpc.protogen.Task> job1TasksOnly = migrationHandler.findTasks(query, Page.first(10));
        assertThat(job1TasksOnly.getItems()).hasSize(2);

        // Now everything
        PageResult<com.netflix.titus.grpc.protogen.Task> allJobs = migrationHandler.findTasks(QUERY_ALL, Page.first(10));
        assertThat(allJobs.getItems()).hasSize(3);
    }

    private Pair<Job<?>, List<Task>> addToJobDataReplicator(Pair<Job<?>, List<Task>> jobAndTasks) {
        JobSnapshot updated = jobDataReplicator.getCurrent().updateJob(jobAndTasks.getLeft()).orElse(jobDataReplicator.getCurrent());
        for (Task task : jobAndTasks.getRight()) {
            updated = updated.updateTask(task, false).orElse(updated);
        }
        when(jobDataReplicator.getCurrent()).thenReturn(updated);
        return jobAndTasks;
    }

    private void setNeedsRelocation(Pair<Job<?>, List<Task>> jobAndTasks, int needsRelocationCount) {
        TaskRelocationSnapshot.Builder builder = relocationDataReplicator.getCurrent().toBuilder();
        List<Task> tasks = jobAndTasks.getRight();
        for (int i = 0; i < needsRelocationCount; i++) {
            builder.addPlan(TaskRelocationPlan.newBuilder()
                    .withTaskId(tasks.get(i).getId())
                    .build()
            );
        }
        when(relocationDataReplicator.getCurrent()).thenReturn(builder.build());
    }

    private static Pair<Job<?>, List<Task>> newJobAndTasks(String jobId, int taskCount) {
        Job<BatchJobExt> job = JobFunctions.changeBatchJobSize(
                JobGenerator.oneBatchJob().toBuilder().withId(jobId).build(),
                taskCount
        );
        List<Task> tasks = (List) JobGenerator.batchTasks(job).getValues(taskCount);
        return Pair.of(job, tasks);
    }
}