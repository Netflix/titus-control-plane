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

package com.netflix.titus.runtime.connector.jobmanager.replicator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.replicator.GrpcJobReplicatorEventStream.CacheUpdater;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactories;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcJobReplicatorEventStreamTest {

    private static final String SERVICE_JOB = "serviceJob";
    private static final String BATCH_JOB = "batchJob";

    private static final int SERVICE_DESIRED = 5;
    private static final int BATCH_DESIRED = 1;

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobDataReplicatorConfiguration configuration = Mockito.mock(JobDataReplicatorConfiguration.class);

    private final JobComponentStub jobServiceStub = new JobComponentStub(titusRuntime);

    private final JobManagementClient client = mock(JobManagementClient.class);

    @Before
    public void setUp() {
        jobServiceStub.addJobTemplate(SERVICE_JOB, JobDescriptorGenerator.serviceJobDescriptors()
                .map(jd -> jd.but(d -> d.getExtensions().toBuilder().withCapacity(Capacity.newBuilder().withDesired(SERVICE_DESIRED).withMax(10).build())))
                .cast(JobDescriptor.class)
        );

        jobServiceStub.addJobTemplate(BATCH_JOB, JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getExtensions().toBuilder().withSize(BATCH_DESIRED)))
                .cast(JobDescriptor.class)
        );
    }

    @Test
    public void testCacheBootstrap() {
        jobServiceStub.creteMultipleJobsAndTasks(SERVICE_JOB, BATCH_JOB);

        newConnectVerifier()
                .assertNext(initialReplicatorEvent -> {
                    assertThat(initialReplicatorEvent).isNotNull();

                    JobSnapshot cache = initialReplicatorEvent.getSnapshot();
                    assertThat(cache.getJobMap()).hasSize(2);
                    assertThat(cache.getTaskMap()).hasSize(SERVICE_DESIRED + BATCH_DESIRED);
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheJobUpdate() {
        Job job = jobServiceStub.createJob(SERVICE_JOB);

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getJobs().get(0).getStatus().getState()).isEqualTo(JobState.Accepted))
                .then(() -> jobServiceStub.moveJobToKillInitiatedState(job))
                .assertNext(next -> assertThat(next.getSnapshot().getJobs().get(0).getStatus().getState()).isEqualTo(JobState.KillInitiated))
                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheJobRemove() {
        Job job = jobServiceStub.createJob(SERVICE_JOB);
        jobServiceStub.moveJobToKillInitiatedState(job);

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getJobs().get(0).getStatus().getState()).isEqualTo(JobState.KillInitiated))
                .then(() -> jobServiceStub.finishJob(job))
                .assertNext(next -> assertThat(next.getSnapshot().getJobs()).isEmpty())

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheTaskUpdate() {
        Pair<Job, List<Task>> pair = jobServiceStub.createJobAndTasks(BATCH_JOB);
        Task task = pair.getRight().get(0);

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getTasks().get(0).getStatus().getState()).isEqualTo(TaskState.Accepted))
                .then(() -> jobServiceStub.moveTaskToState(task, TaskState.Launched))
                .assertNext(next -> assertThat(next.getSnapshot().getTasks().get(0).getStatus().getState()).isEqualTo(TaskState.Launched))

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheTaskMove() {
        Pair<Job, List<Task>> pair = jobServiceStub.createJobAndTasks(SERVICE_JOB);
        Job target = jobServiceStub.createJob(SERVICE_JOB);
        Task task = pair.getRight().get(0);
        String sourceJobId = pair.getLeft().getId();
        String targetJobId = target.getId();
        List<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> events = new ArrayList<>();

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getTaskMap().values())
                        .allSatisfy(t -> assertThat(t.getStatus().getState()).isEqualTo(TaskState.Accepted)))
                .then(() -> jobServiceStub.moveTaskToState(task, TaskState.Started))
                .assertNext(next -> {
                    JobSnapshot snapshot = next.getSnapshot();
                    Optional<Pair<Job<?>, Task>> taskOpt = snapshot.findTaskById(task.getId());
                    assertThat(taskOpt).isPresent();
                    assertThat(taskOpt.get().getRight().getStatus().getState()).isEqualTo(TaskState.Started);
                    assertThat(snapshot.getTasks(sourceJobId)).containsKey(task.getId());
                })
                .then(() -> jobServiceStub.getJobOperations()
                        .moveServiceTask(sourceJobId, targetJobId, task.getId(), CallMetadata.newBuilder().withCallerId("Test").withCallReason("testing").build())
                        .test()
                        .awaitTerminalEvent()
                        .assertNoErrors()
                )
                .recordWith(() -> events)
                .thenConsumeWhile(next -> {
                    JobManagerEvent<?> trigger = next.getTrigger();
                    if (!(trigger instanceof TaskUpdateEvent)) {
                        return true;
                    }
                    TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) trigger;
                    return !taskUpdateEvent.isMovedFromAnotherJob();
                })
                .thenCancel()
                .verify();

        assertThat(events).hasSize(3);
        events.stream().map(ReplicatorEvent::getTrigger).forEach(jobManagerEvent -> {
            if (jobManagerEvent instanceof JobUpdateEvent) {
                JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) jobManagerEvent;
                String eventJobId = jobUpdateEvent.getCurrent().getId();
                assertThat(eventJobId).isIn(sourceJobId, targetJobId);
            } else if (jobManagerEvent instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) jobManagerEvent;
                assertThat(taskUpdateEvent.isMovedFromAnotherJob()).isTrue();
                assertThat(taskUpdateEvent.getCurrentJob().getId()).isEqualTo(targetJobId);
                assertThat(taskUpdateEvent.getCurrent().getJobId()).isEqualTo(targetJobId);
                assertThat(taskUpdateEvent.getCurrent().getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB))
                        .isEqualTo(sourceJobId);
            } else {
                fail("Unexpected event type: %s", jobManagerEvent);
            }
        });
    }

    @Test
    public void testCacheTaskRemove() {
        Pair<Job, List<Task>> pair = jobServiceStub.createJobAndTasks(SERVICE_JOB);
        List<Task> tasks = pair.getRight();
        Task task = tasks.get(0);

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getTasks().get(0).getStatus().getState()).isEqualTo(TaskState.Accepted))
                .then(() -> jobServiceStub.moveTaskToState(task, TaskState.Finished))
                .assertNext(next -> assertThat(next.getSnapshot().getTaskMap()).hasSize(tasks.size() - 1))
                .thenCancel()
                .verify();
    }

    @Test
    public void testReemit() {
        jobServiceStub.creteMultipleJobsAndTasks(SERVICE_JOB, BATCH_JOB);

        newConnectVerifier()
                .expectNextCount(1)
                .expectNoEvent(Duration.ofMillis(GrpcJobReplicatorEventStream.LATENCY_REPORT_INTERVAL_MS))
                .expectNextCount(1)

                .thenCancel()
                .verify();
    }

    @Test
    public void testConnectionTimeout() {
        when(configuration.getConnectionTimeoutMs()).thenReturn(30_000L);
        when(configuration.isConnectionTimeoutEnabled()).thenReturn(true);

        jobServiceStub.creteMultipleJobsAndTasks(SERVICE_JOB, BATCH_JOB);
        jobServiceStub.delayConnection();
        newConnectVerifier()
                .thenAwait(Duration.ofMillis(30_000))
                .expectError(TimeoutException.class)
                .verify();
    }

    @Test
    public void testCacheSnapshotFiltersCompletedJobs() {
        Job<?> acceptedJob = JobGenerator.oneBatchJob();
        BatchJobTask acceptedTask = JobGenerator.oneBatchTask().toBuilder()
                .withJobId(acceptedJob.getId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .build();

        Job<?> finishedJob = JobFunctions.changeJobStatus(acceptedJob, JobStatus.newBuilder().withState(JobState.Finished).build());
        Task finishedTask = JobFunctions.changeTaskStatus(acceptedTask, TaskStatus.newBuilder().withState(TaskState.Finished).build());

        CacheUpdater cacheUpdater = new CacheUpdater(JobSnapshotFactories.newDefault(titusRuntime), titusRuntime);
        assertThat(cacheUpdater.onEvent(JobUpdateEvent.newJob(acceptedJob, CallMetadataConstants.UNDEFINED_CALL_METADATA))).isEmpty();
        assertThat(cacheUpdater.onEvent(TaskUpdateEvent.newTask(acceptedJob, acceptedTask, CallMetadataConstants.UNDEFINED_CALL_METADATA))).isEmpty();
        assertThat(cacheUpdater.onEvent(TaskUpdateEvent.taskChange(acceptedJob, finishedTask, acceptedTask, CallMetadataConstants.UNDEFINED_CALL_METADATA))).isEmpty();
        assertThat(cacheUpdater.onEvent(JobUpdateEvent.jobChange(finishedJob, acceptedJob, CallMetadataConstants.UNDEFINED_CALL_METADATA))).isEmpty();
        ReplicatorEvent<JobSnapshot, JobManagerEvent<?>> snapshotEvent = cacheUpdater.onEvent(JobManagerEvent.snapshotMarker()).orElse(null);
        assertThat(snapshotEvent).isNotNull();
        assertThat(snapshotEvent.getSnapshot().getJobMap()).isEmpty();
        assertThat(snapshotEvent.getSnapshot().getTaskMap()).isEmpty();
    }

    private GrpcJobReplicatorEventStream newStream() {
        when(client.observeJobs(any())).thenReturn(ReactorExt.toFlux(jobServiceStub.observeJobs(true)));
        return new GrpcJobReplicatorEventStream(client, JobSnapshotFactories.newDefault(titusRuntime), configuration, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel());
    }

    private StepVerifier.FirstStep<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }
}
