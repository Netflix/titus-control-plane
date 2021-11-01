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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.LogLocation;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactories;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.gateway.service.v3.internal.LocalCacheQueryProcessor.PARAMETER_USE_CACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalCacheQueryProcessorTest {

    private static final JobQuery JOB_QUERY_ALL_WITH_PAGE_SIZE_2 = JobQuery.newBuilder()
            .setPage(Page.newBuilder().setPageSize(2).build())
            .build();

    private static final TaskQuery TASK_QUERY_ALL_WITH_PAGE_SIZE_2 = TaskQuery.newBuilder()
            .setPage(Page.newBuilder().setPageSize(2).build())
            .build();

    private static final CallMetadata JUNIT_CALL_METADATA = CallMetadata.newBuilder().withCallerId("junit").build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final SettableConfig configurationMap = new DefaultSettableConfig();
    private final GatewayConfiguration configuration = Archaius2Ext.newConfiguration(GatewayConfiguration.class, configurationMap);

    private final JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);

    private final LocalCacheQueryProcessor processor = new LocalCacheQueryProcessor(
            configuration,
            jobDataReplicator,
            EmptyLogStorageInfo.empty(),
            Schedulers.immediate(),
            titusRuntime
    );

    private final Sinks.Many<Pair<JobSnapshot, JobManagerEvent<?>>> jobDataReplicatorSink = Sinks.many().multicast().directAllOrNothing();
    private final Sinks.Many<Long> jobDataReplicatorCheckpointSink = Sinks.many().multicast().directAllOrNothing();

    @Before
    public void setUp() {
        when(jobDataReplicator.getCurrent()).thenReturn(JobSnapshotFactories.newDefaultEmptySnapshot(titusRuntime));
        when(jobDataReplicator.events()).thenReturn(jobDataReplicatorSink.asFlux());
        when(jobDataReplicator.observeLastCheckpointTimestamp()).thenReturn(jobDataReplicatorCheckpointSink.asFlux());
    }

    @Test
    public void testCanUseCacheByDirectRequest() {
        assertThat(processor.canUseCache(Collections.emptyMap(), "anything", CallMetadataConstants.UNDEFINED_CALL_METADATA)).isFalse();
        assertThat(processor.canUseCache(Collections.singletonMap(PARAMETER_USE_CACHE, "true"), "anything", CallMetadataConstants.UNDEFINED_CALL_METADATA)).isTrue();
    }

    @Test
    public void testCanUseCacheByCallerId() {
        assertThat(processor.canUseCache(Collections.emptyMap(), "anything", JUNIT_CALL_METADATA)).isFalse();
        configurationMap.setProperty("titusGateway.queryFromCacheCallerId", "junit.*");
        assertThat(processor.canUseCache(Collections.emptyMap(), "anything", JUNIT_CALL_METADATA)).isTrue();
    }

    @Test
    public void testFindJob() {
        Job<?> job1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getLeft();
        Job<?> job2 = addToJobDataReplicator(newJobAndTasks("job2", 4)).getLeft();

        for (Job<?> job : Arrays.asList(job1, job2)) {
            com.netflix.titus.grpc.protogen.Job result = processor.findJob(job.getId()).orElse(null);
            assertThat(result).isNotNull();
            assertThat(result.getId()).isEqualTo(job.getId());
        }
    }

    @Test
    public void testFindJobs() {
        Job<?> job1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getLeft();
        Job<?> job2 = addToJobDataReplicator(newJobAndTasks("job2", 4)).getLeft();
        Job<?> job3 = addToJobDataReplicator(newJobAndTasks("job3", 0)).getLeft();

        // Expect two pages
        JobQueryResult page1Result = processor.findJobs(JOB_QUERY_ALL_WITH_PAGE_SIZE_2);
        assertThat(page1Result.getItemsList()).hasSize(2);

        Page page2 = Page.newBuilder().setPageSize(2).setCursor(page1Result.getPagination().getCursor()).build();
        JobQueryResult page2Result = processor.findJobs(JobQuery.newBuilder().setPage(page2).build());
        assertThat(page2Result.getItemsList()).hasSize(1);
        assertThat(page2Result.getPagination().getHasMore()).isFalse();

        List<String> jobIds = CollectionsExt.merge(
                page1Result.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Job::getId).collect(Collectors.toList()),
                page2Result.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Job::getId).collect(Collectors.toList())
        );
        assertThat(jobIds).contains(job1.getId(), job2.getId(), job3.getId());
    }

    @Test
    public void testFindJobsWithFieldsFilter() {
        Job<?> job1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getLeft();
        JobQueryResult page1Result = processor.findJobs(JobQuery.newBuilder()
                .addFields("status")
                .setPage(Page.newBuilder().setPageSize(2).build())
                .build()
        );
        assertThat(page1Result.getItemsList()).hasSize(1);
        com.netflix.titus.grpc.protogen.Job fetchedJob = page1Result.getItemsList().get(0);

        // Check that expected fields are set
        assertThat(fetchedJob.getId()).isEqualTo(job1.getId());
        assertThat(fetchedJob.getStatus().getReasonMessage()).isEqualTo(job1.getStatus().getReasonMessage());

        // Check that not requested fields are not set
        assertThat(fetchedJob.getJobDescriptor()).isEqualTo(com.netflix.titus.grpc.protogen.JobDescriptor.getDefaultInstance());
    }

    @Test
    public void testFindTasks() {
        List<Task> tasks1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getRight();
        List<Task> tasks2 = addToJobDataReplicator(newJobAndTasks("job2", 4)).getRight();
        List<Task> tasks3 = addToJobDataReplicator(newJobAndTasks("job3", 0)).getRight();

        // Expect three pages
        TaskQueryResult page1Result = processor.findTasks(TASK_QUERY_ALL_WITH_PAGE_SIZE_2);
        assertThat(page1Result.getItemsList()).hasSize(2);

        Page page2 = Page.newBuilder().setPageSize(2).setCursor(page1Result.getPagination().getCursor()).build();
        TaskQueryResult page2Result = processor.findTasks(TaskQuery.newBuilder().setPage(page2).build());
        assertThat(page2Result.getItemsList()).hasSize(2);

        Page page3 = Page.newBuilder().setPageSize(2).setCursor(page2Result.getPagination().getCursor()).build();
        TaskQueryResult page3Result = processor.findTasks(TaskQuery.newBuilder().setPage(page3).build());
        assertThat(page3Result.getItemsList()).hasSize(2);
        assertThat(page3Result.getPagination().getHasMore()).isFalse();

        List<String> taskIds = CollectionsExt.merge(
                page1Result.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Task::getId).collect(Collectors.toList()),
                page2Result.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Task::getId).collect(Collectors.toList()),
                page3Result.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Task::getId).collect(Collectors.toList())
        );
        List<String> expectedTaskIds = CollectionsExt.merge(
                tasks1.stream().map(Task::getId).collect(Collectors.toList()),
                tasks2.stream().map(Task::getId).collect(Collectors.toList()),
                tasks3.stream().map(Task::getId).collect(Collectors.toList())
        );
        assertThat(taskIds).containsAll(expectedTaskIds);
    }

    @Test
    public void testFindTasksWithFieldsFilter() {
        List<Task> tasks1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getRight();
        TaskQueryResult page1Result = processor.findTasks(TaskQuery.newBuilder()
                .addFields("status")
                .setPage(Page.newBuilder().setPageSize(2).build())
                .build()
        );
        assertThat(page1Result.getItemsList()).hasSize(2);

        Task originalTask = tasks1.get(0);
        com.netflix.titus.grpc.protogen.Task fetchedTask = page1Result.getItemsList().stream()
                .filter(t -> t.getId().equals(originalTask.getId()))
                .findFirst().get();

        // Check that expected fields are set
        assertThat(fetchedTask.getId()).isEqualTo(originalTask.getId());
        assertThat(fetchedTask.getStatus().getReasonMessage()).isEqualTo(originalTask.getStatus().getReasonMessage());

        // Check that not requested fields are not set
        assertThat(fetchedTask.getLogLocation()).isEqualTo(LogLocation.getDefaultInstance());
    }

    @Test
    public void testFindTask() {
        Task task1 = addToJobDataReplicator(newJobAndTasks("job1", 2)).getRight().get(0);
        Task task2 = addToJobDataReplicator(newJobAndTasks("job2", 4)).getRight().get(0);

        for (Task task : Arrays.asList(task1, task2)) {
            com.netflix.titus.grpc.protogen.Task result = processor.findTask(task.getId()).orElse(null);
            assertThat(result).isNotNull();
            assertThat(result.getId()).isEqualTo(task.getId());
        }
    }

    @Test
    public void testObserveJobsEmitsEmptySnapshotIfNoJobsAreRunning() throws InterruptedException {
        ExtTestSubscriber<JobChangeNotification> subscriber = new ExtTestSubscriber<>();
        processor.observeJobs(ObserveJobsQuery.getDefaultInstance()).subscribe(subscriber);

        emitEvent(Pair.of(jobDataReplicator.getCurrent(), JobManagerEvent.snapshotMarker()));

        JobChangeNotification receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.SNAPSHOTEND);
    }

    @Test
    public void testObserveJobs() throws InterruptedException {
        ExtTestSubscriber<JobChangeNotification> subscriber = new ExtTestSubscriber<>();
        processor.observeJobs(ObserveJobsQuery.getDefaultInstance()).subscribe(subscriber);

        Pair<Job<?>, List<Task>> jobAndTasks = addToJobDataReplicator(newJobAndTasks("job1", 2));
        Job<?> job = jobAndTasks.getLeft();
        Task task1 = jobAndTasks.getRight().get(0);
        Task task2 = jobAndTasks.getRight().get(1);

        // Job update event, which also triggers snapshot
        JobUpdateEvent jobUpdateEvent = JobUpdateEvent.newJob(job, JUNIT_CALL_METADATA);
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), jobUpdateEvent));
        JobChangeNotification receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.SNAPSHOTEND);
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);

        // Task update event
        TaskUpdateEvent taskUpdateEvent = TaskUpdateEvent.newTask(job, task1, JUNIT_CALL_METADATA);
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), taskUpdateEvent));
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);

        // Job replicator re-sends events if there is nothing new to keep the stream active. Make sure that
        // we filter the keep alive events.
        TaskUpdateEvent taskUpdateEvent2 = TaskUpdateEvent.newTask(job, task2, JUNIT_CALL_METADATA);
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), JobManagerEvent.keepAliveEvent(-1)));
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), taskUpdateEvent2));
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getTaskUpdate().getTask().getId()).isEqualTo(task2.getId());

        // Now repeat taskUpdateEvent which this time should go through.
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), taskUpdateEvent));
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getTaskUpdate().getTask().getId()).isEqualTo(task1.getId());

        // Check that is correctly terminated
        jobDataReplicatorSink.tryEmitError(new RuntimeException("simulated stream error"));
        await().until(() -> subscriber.getError() != null);
        assertThat(subscriber.getError()).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testObserveJob() throws InterruptedException {
        ExtTestSubscriber<JobChangeNotification> subscriber = new ExtTestSubscriber<>();
        processor.observeJob("job1").subscribe(subscriber);

        Pair<Job<?>, List<Task>> jobAndTasks = addToJobDataReplicator(newJobAndTasks("job1", 2));
        Job<?> job2 = addToJobDataReplicator(newJobAndTasks("job2", 2)).getLeft();
        Job<?> job1 = jobAndTasks.getLeft();
        Task task1 = jobAndTasks.getRight().get(0);
        Task task2 = jobAndTasks.getRight().get(1);

        // Job update event, which also triggers snapshot
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), JobUpdateEvent.newJob(job2, JUNIT_CALL_METADATA)));
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), JobUpdateEvent.newJob(job1, JUNIT_CALL_METADATA)));

        JobChangeNotification receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);
        assertThat(receivedEvent.getJobUpdate().getJob().getId()).isEqualTo(job1.getId());
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        assertThat(receivedEvent.getTaskUpdate().getTask().getJobId()).isEqualTo(job1.getId());
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        assertThat(receivedEvent.getTaskUpdate().getTask().getJobId()).isEqualTo(job1.getId());
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.SNAPSHOTEND);
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);
        assertThat(receivedEvent.getJobUpdate().getJob().getId()).isEqualTo(job1.getId());

        // Task update event
        TaskUpdateEvent taskUpdateEvent = TaskUpdateEvent.newTask(job1, task1, JUNIT_CALL_METADATA);
        emitEvent(Pair.of(jobDataReplicator.getCurrent(), taskUpdateEvent));
        receivedEvent = subscriber.takeNext(30, TimeUnit.SECONDS);
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        assertThat(receivedEvent.getTaskUpdate().getTask().getId()).isEqualTo(task1.getId());

        // Check that is correctly terminated
        jobDataReplicatorSink.tryEmitError(new RuntimeException("simulated stream error"));
        await().until(() -> subscriber.getError() != null);
        assertThat(subscriber.getError()).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testSync() {
        long startTime = titusRuntime.getClock().wallTime() - 10;

        ExtTestSubscriber<Task> subscriber = new ExtTestSubscriber<>();
        processor.syncCache("test", Task.class).subscribe(subscriber);

        assertThat(subscriber.isUnsubscribed()).isFalse();

        // Move forward, but not ahead of the current time threshold
        jobDataReplicatorCheckpointSink.emitNext(startTime + 1, Sinks.EmitFailureHandler.FAIL_FAST);
        assertThat(subscriber.isUnsubscribed()).isFalse();

        // Now pass the threshold
        jobDataReplicatorCheckpointSink.emitNext(startTime + 60_000, Sinks.EmitFailureHandler.FAIL_FAST);
        await().until(subscriber::isUnsubscribed);
    }

    private Pair<Job<?>, List<Task>> addToJobDataReplicator(Pair<Job<?>, List<Task>> jobAndTasks) {
        JobSnapshot updated = jobDataReplicator.getCurrent().updateJob(jobAndTasks.getLeft()).orElse(jobDataReplicator.getCurrent());
        for (Task task : jobAndTasks.getRight()) {
            updated = updated.updateTask(task, false).orElse(updated);
        }
        when(jobDataReplicator.getCurrent()).thenReturn(updated);
        return jobAndTasks;
    }

    private static Pair<Job<?>, List<Task>> newJobAndTasks(String jobId, int taskCount) {
        Job<BatchJobExt> job = JobFunctions.changeBatchJobSize(
                JobGenerator.oneBatchJob().toBuilder().withId(jobId).build(),
                taskCount
        );
        List<Task> tasks = (List) JobGenerator.batchTasks(job).getValues(taskCount);
        return Pair.of(job, tasks);
    }

    private void emitEvent(Pair<JobSnapshot, JobManagerEvent<?>> e) {
        jobDataReplicatorSink.emitNext(e, Sinks.EmitFailureHandler.FAIL_FAST);
    }
}