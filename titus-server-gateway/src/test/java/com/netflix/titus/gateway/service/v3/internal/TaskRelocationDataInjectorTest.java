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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.gateway.kubernetes.KubeApiConnector;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskRelocationDataInjectorTest {

    private static final long REQUEST_TIMEOUT_MS = 1_000L;
    private static final long RELOCATION_TIMEOUT_MS = REQUEST_TIMEOUT_MS / 2;

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private static final Task TASK1 = GrpcJobManagementModelConverters.toGrpcTask(JobGenerator.oneBatchTask().toBuilder().withId("task1").build(), EmptyLogStorageInfo.empty());
    private static final Task TASK2 = GrpcJobManagementModelConverters.toGrpcTask(JobGenerator.oneBatchTask().toBuilder().withId("task2").build(), EmptyLogStorageInfo.empty());

    private final GrpcClientConfiguration grpcConfiguration = mock(GrpcClientConfiguration.class);
    private final JobManagerConfiguration jobManagerConfiguration = mock(JobManagerConfiguration.class);
    private final FeatureActivationConfiguration featureActivationConfiguration = mock(FeatureActivationConfiguration.class);

    private final RelocationServiceClient relocationServiceClient = mock(RelocationServiceClient.class);
    private final RelocationDataReplicator relocationDataReplicator = mock(RelocationDataReplicator.class);
    private final KubeApiConnector kubeApiConnector = mock(KubeApiConnector.class);

    private final TaskRelocationDataInjector taskRelocationDataInjector = new TaskRelocationDataInjector(
            grpcConfiguration,
            jobManagerConfiguration,
            featureActivationConfiguration,
            relocationServiceClient,
            relocationDataReplicator,
            kubeApiConnector,
            testScheduler
    );

    @Before
    public void setUp() {
        when(grpcConfiguration.getRequestTimeout()).thenReturn(REQUEST_TIMEOUT_MS);
        when(jobManagerConfiguration.getRelocationTimeoutCoefficient()).thenReturn(0.5);
        when(featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()).thenReturn(true);
    }

    @Test
    public void testTaskUpdateEventWithRelocationDeadline() {
        long deadlineTimestamp = titusRuntime.getClock().wallTime() + 1_000;

        when(relocationDataReplicator.getCurrent()).thenReturn(
                newRelocationSnapshot(newRelocationPlan(TASK1, deadlineTimestamp))
        );

        JobChangeNotification event = JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(TASK1).build())
                .build();
        JobChangeNotification updatedEvent = taskRelocationDataInjector.injectIntoTaskUpdateEvent(event);
        Task merged = updatedEvent.getTaskUpdate().getTask();
        assertThat(merged.getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getMigrationDetails().getDeadline()).isEqualTo(deadlineTimestamp);
    }

    @Test
    public void testTaskUpdateEventWithoutRelocationDeadline() {
        when(relocationDataReplicator.getCurrent()).thenReturn(TaskRelocationSnapshot.empty());
        JobChangeNotification event = JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(TASK1).build())
                .build();
        JobChangeNotification updatedEvent = taskRelocationDataInjector.injectIntoTaskUpdateEvent(event);
        assertThat(updatedEvent).isEqualTo(event);
    }

    private TaskRelocationSnapshot newRelocationSnapshot(TaskRelocationPlan plan) {
        return TaskRelocationSnapshot.newBuilder().addPlan(plan).build();
    }

    @Test
    public void testFindTaskWithRelocationDeadline() {
        long deadlineTimestamp = titusRuntime.getClock().wallTime() + 1_000;

        when(relocationServiceClient.findTaskRelocationPlan(TASK1.getId())).thenReturn(
                Mono.just(Optional.of(newRelocationPlan(TASK1, deadlineTimestamp)))
        );

        Task merged = taskRelocationDataInjector.injectIntoTask(TASK1.getId(), Observable.just(TASK1)).toBlocking().first();
        assertThat(merged.getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getMigrationDetails().getDeadline()).isEqualTo(deadlineTimestamp);
    }

    @Test
    public void testFindTaskWithoutRelocationDeadline() {
        when(relocationServiceClient.findTaskRelocationPlan(TASK1.getId())).thenReturn(Mono.just(Optional.empty()));
        Task merged = taskRelocationDataInjector.injectIntoTask(TASK1.getId(), Observable.just(TASK1)).toBlocking().first();
        assertThat(merged.getMigrationDetails().getNeedsMigration()).isFalse();
    }

    @Test
    public void testFindTaskWithLegacyMigration() {
        long deadlineTimestamp = titusRuntime.getClock().wallTime() + 1_000;

        Task legacyTask = toLegacyTask(TASK1, deadlineTimestamp);

        when(relocationServiceClient.findTaskRelocationPlan(TASK1.getId())).thenReturn(Mono.just(Optional.empty()));
        taskRelocationDataInjector.injectIntoTask(legacyTask.getId(), Observable.just(legacyTask)).toBlocking().first();

        assertThat(legacyTask).isEqualToComparingFieldByField(legacyTask);
    }

    @Test
    public void testFindTaskWithRelocationDataFetchTimeout() {
        when(relocationServiceClient.findTaskRelocationPlan(TASK1.getId())).thenReturn(Mono.never());

        ExtTestSubscriber<Task> testSubscriber = new ExtTestSubscriber<>();
        taskRelocationDataInjector.injectIntoTask(TASK1.getId(), Observable.just(TASK1)).subscribe(testSubscriber);

        testSubscriber.assertOpen();
        testScheduler.advanceTimeBy(RELOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isEqualTo(TASK1);
        testSubscriber.assertOnCompleted();
    }

    @Test
    public void testFindTasksWithRelocationDeadline() {
        long deadline1 = titusRuntime.getClock().wallTime() + 1_000;
        long deadline2 = titusRuntime.getClock().wallTime() + 2_000;

        TaskQueryResult queryResult = TaskQueryResult.newBuilder()
                .addItems(TASK1)
                .addItems(TASK2)
                .build();
        List<TaskRelocationPlan> relocationPlans = Arrays.asList(
                newRelocationPlan(TASK1, deadline1),
                newRelocationPlan(TASK2, deadline2)
        );
        when(relocationServiceClient.findTaskRelocationPlans(asSet(TASK1.getId(), TASK2.getId()))).thenReturn(Mono.just(relocationPlans));

        TaskQueryResult merged = taskRelocationDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

        assertThat(merged.getItems(0).getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getItems(0).getMigrationDetails().getDeadline()).isEqualTo(deadline1);

        assertThat(merged.getItems(1).getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getItems(1).getMigrationDetails().getDeadline()).isEqualTo(deadline2);
    }

    @Test
    public void testFindTasksWithoutRelocationDeadline() {
        long deadline1 = titusRuntime.getClock().wallTime() + 1_000;

        TaskQueryResult queryResult = TaskQueryResult.newBuilder()
                .addItems(TASK1)
                .addItems(TASK2)
                .build();
        List<TaskRelocationPlan> relocationPlans = Collections.singletonList(newRelocationPlan(TASK1, deadline1));

        when(relocationServiceClient.findTaskRelocationPlans(asSet(TASK1.getId(), TASK2.getId()))).thenReturn(Mono.just(relocationPlans));

        TaskQueryResult merged = taskRelocationDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

        assertThat(merged.getItems(0).getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getItems(0).getMigrationDetails().getDeadline()).isEqualTo(deadline1);

        assertThat(merged.getItems(1).getMigrationDetails().getNeedsMigration()).isFalse();
    }

    @Test
    public void testFindTasksWithLegacyMigration() {
        long deadline1 = titusRuntime.getClock().wallTime() + 1_000;

        long legacyDeadline2 = titusRuntime.getClock().wallTime() + 2_000;
        Task legacyTask = toLegacyTask(TASK2, legacyDeadline2);

        TaskQueryResult queryResult = TaskQueryResult.newBuilder()
                .addItems(TASK1)
                .addItems(legacyTask)
                .build();
        List<TaskRelocationPlan> relocationPlans = Collections.singletonList(newRelocationPlan(TASK1, deadline1));
        when(relocationServiceClient.findTaskRelocationPlans(asSet(TASK1.getId(), TASK2.getId()))).thenReturn(Mono.just(relocationPlans));

        TaskQueryResult merged = taskRelocationDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

        assertThat(merged.getItems(0).getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getItems(0).getMigrationDetails().getDeadline()).isEqualTo(deadline1);

        assertThat(merged.getItems(1)).isEqualTo(legacyTask);
    }

    @Test
    public void testFindTasksWithRelocationDataFetchTimeout() {
        when(relocationServiceClient.findTaskRelocationPlans(asSet(TASK1.getId(), TASK2.getId()))).thenReturn(Mono.never());

        TaskQueryResult queryResult = TaskQueryResult.newBuilder()
                .addItems(TASK1)
                .addItems(TASK2)
                .build();

        ExtTestSubscriber<TaskQueryResult> testSubscriber = new ExtTestSubscriber<>();
        taskRelocationDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).subscribe(testSubscriber);

        testSubscriber.assertOpen();
        testScheduler.advanceTimeBy(RELOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        testSubscriber.assertOnCompleted();

        TaskQueryResult result = testSubscriber.takeNext();
        assertThat(result.getItemsList()).contains(TASK1, TASK2);
    }

    private Task toLegacyTask(Task task, long deadlineTimestamp) {
        return task.toBuilder().setMigrationDetails(MigrationDetails.newBuilder()
                .setNeedsMigration(true)
                .setDeadline(deadlineTimestamp)
                .build()
        ).build();
    }

    private TaskRelocationPlan newRelocationPlan(Task task, long deadlineTimestamp) {
        return TaskRelocationPlan.newBuilder()
                .withTaskId(task.getId())
                .withReason(TaskRelocationReason.TaskMigration)
                .withRelocationTime(deadlineTimestamp)
                .build();
    }
}