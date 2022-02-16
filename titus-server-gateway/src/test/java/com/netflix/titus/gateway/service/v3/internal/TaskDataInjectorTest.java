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
import java.util.HashMap;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.grpc.protogen.BasicImage;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOConnector;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.gateway.service.v3.internal.TaskDataInjector.buildBasicImageFromContainerStatus;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_IMAGE_TAG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskDataInjectorTest {

    private static final long REQUEST_TIMEOUT_MS = 1_000L;

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private static final Task TASK1 = GrpcJobManagementModelConverters.toGrpcTask(JobGenerator.oneBatchTask().toBuilder().withId("task1").build(), EmptyLogStorageInfo.empty());
    private static final Task TASK2 = GrpcJobManagementModelConverters.toGrpcTask(JobGenerator.oneBatchTask().toBuilder().withId("task2").build(), EmptyLogStorageInfo.empty());

    private final GrpcClientConfiguration grpcConfiguration = mock(GrpcClientConfiguration.class);
    private final FeatureActivationConfiguration featureActivationConfiguration = mock(FeatureActivationConfiguration.class);

    private final RelocationDataReplicator relocationDataReplicator = mock(RelocationDataReplicator.class);
    private final Fabric8IOConnector kubeApiConnector = mock(Fabric8IOConnector.class);

    private final TaskDataInjector taskDataInjector = new TaskDataInjector(featureActivationConfiguration, relocationDataReplicator, kubeApiConnector);

    @Before
    public void setUp() {
        when(grpcConfiguration.getRequestTimeout()).thenReturn(REQUEST_TIMEOUT_MS);
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
        JobChangeNotification updatedEvent = taskDataInjector.injectIntoTaskUpdateEvent(event);
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
        JobChangeNotification updatedEvent = taskDataInjector.injectIntoTaskUpdateEvent(event);
        assertThat(updatedEvent).isEqualTo(event);
    }

    private TaskRelocationSnapshot newRelocationSnapshot(TaskRelocationPlan... plans) {
        TaskRelocationSnapshot.Builder builder = TaskRelocationSnapshot.newBuilder();
        for (TaskRelocationPlan plan : plans) {
            builder.addPlan(plan);
        }
        return builder.build();
    }

    @Test
    public void testFindTaskWithRelocationDeadline() {
        long deadlineTimestamp = titusRuntime.getClock().wallTime() + 1_000;

        when(relocationDataReplicator.getCurrent()).thenReturn(
                newRelocationSnapshot(newRelocationPlan(TASK1, deadlineTimestamp))
        );

        Task merged = taskDataInjector.injectIntoTask(Observable.just(TASK1)).toBlocking().first();
        assertThat(merged.getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getMigrationDetails().getDeadline()).isEqualTo(deadlineTimestamp);
    }

    @Test
    public void testFindTaskWithoutRelocationDeadline() {
        when(relocationDataReplicator.getCurrent()).thenReturn(TaskRelocationSnapshot.empty());
        Task merged = taskDataInjector.injectIntoTask(Observable.just(TASK1)).toBlocking().first();
        assertThat(merged.getMigrationDetails().getNeedsMigration()).isFalse();
    }

    @Test
    public void testFindTaskWithLegacyMigration() {
        long deadlineTimestamp = titusRuntime.getClock().wallTime() + 1_000;

        Task legacyTask = toLegacyTask(TASK1, deadlineTimestamp);

        when(relocationDataReplicator.getCurrent()).thenReturn(TaskRelocationSnapshot.empty());
        taskDataInjector.injectIntoTask(Observable.just(legacyTask)).toBlocking().first();

        assertThat(legacyTask).isEqualToComparingFieldByField(legacyTask);
    }

    @Test
    public void testFindTasksWithRelocationDeadline() {
        long deadline1 = titusRuntime.getClock().wallTime() + 1_000;
        long deadline2 = titusRuntime.getClock().wallTime() + 2_000;

        TaskQueryResult queryResult = TaskQueryResult.newBuilder()
                .addItems(TASK1)
                .addItems(TASK2)
                .build();
        when(relocationDataReplicator.getCurrent()).thenReturn(newRelocationSnapshot(
                newRelocationPlan(TASK1, deadline1),
                newRelocationPlan(TASK2, deadline2)
        ));

        TaskQueryResult merged = taskDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

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
        when(relocationDataReplicator.getCurrent()).thenReturn(newRelocationSnapshot(newRelocationPlan(TASK1, deadline1)));
        TaskQueryResult merged = taskDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

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
        when(relocationDataReplicator.getCurrent()).thenReturn(newRelocationSnapshot(newRelocationPlan(TASK1, deadline1)));
        TaskQueryResult merged = taskDataInjector.injectIntoTaskQueryResult(Observable.just(queryResult)).toBlocking().first();

        assertThat(merged.getItems(0).getMigrationDetails().getNeedsMigration()).isTrue();
        assertThat(merged.getItems(0).getMigrationDetails().getDeadline()).isEqualTo(deadline1);

        assertThat(merged.getItems(1)).isEqualTo(legacyTask);
    }

    @Test
    public void testBuildBasicImageFromContainerStatus() {
        Pod pod = new Pod();
        ObjectMeta metadata = new ObjectMeta();
        HashMap<String, String> annotations = new HashMap<>();
        annotations.put(ANNOTATION_KEY_IMAGE_TAG_PREFIX + "container1", "container1sTag");
        metadata.setAnnotations(annotations);
        pod.setMetadata(metadata);
        String imageWithDigest = "registry.example.com/container1Image@123456";
        String imageWithTag = "registry.example.com/container2Image:mytag";

        ContainerStatus containerStatus1 = new ContainerStatus();
        containerStatus1.setName("container1");
        containerStatus1.setImage(imageWithDigest);
        ContainerStatus containerStatus2 = new ContainerStatus();
        containerStatus2.setName("container2");
        containerStatus2.setImage(imageWithTag);
        PodStatus status = new PodStatus();
        status.setContainerStatuses(Arrays.asList(containerStatus1, containerStatus2));
        pod.setStatus(status);

        BasicImage bi1 = buildBasicImageFromContainerStatus(pod, imageWithDigest, "container1");
        assertThat(bi1.getName()).isEqualTo("container1Image");
        assertThat(bi1.getTag()).isEqualTo("container1sTag");
        assertThat(bi1.getDigest()).isEqualTo("123456");

        BasicImage bi2 = buildBasicImageFromContainerStatus(pod, imageWithTag, "container2");
        assertThat(bi2.getName()).isEqualTo("container2Image");
        assertThat(bi2.getTag()).isEqualTo("mytag");
        assertThat(bi2.getDigest()).isEqualTo("");
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