/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.kubeapiserver.ContainerResultCodeResolver;
import com.netflix.titus.master.mesos.kubeapiserver.KubeJobManagementReconciler;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodPhase;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodWrapper;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.TITUS_NODE_DOMAIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KubeNotificationProcessorTest {

    private static final Job<BatchJobExt> JOB = JobGenerator.oneBatchJob();
    private static final BatchJobTask TASK = JobGenerator.oneBatchTask().toBuilder()
            .withTaskContext(CollectionsExt.asMap(TaskAttributes.TASK_ATTRIBUTES_OWNED_BY_KUBE_SCHEDULER, "true"))
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private DirectProcessor<PodEvent> podEvents;
    private DirectProcessor<PodEvent> reconcilerPodEvents;
    private DirectProcessor<ContainerEvent> reconcilerContainerEvents;
    private KubeNotificationProcessor processor;

    @Mock
    private V3JobOperations jobOperations;
    @Mock
    private ContainerResultCodeResolver containerResultCodeResolver;
    @Captor
    private ArgumentCaptor<Function<Task, Optional<Task>>> changeFunctionCaptor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        podEvents = DirectProcessor.create();
        reconcilerPodEvents = DirectProcessor.create();
        reconcilerContainerEvents = DirectProcessor.create();
        processor = new KubeNotificationProcessor(mock(JobManagerConfiguration.class),
                new FakeDirectKube(),
                new FakeReconciler(),
                jobOperations,
                containerResultCodeResolver,
                titusRuntime
        );
        processor.enterActiveMode();

        when(jobOperations.findTaskById(eq(TASK.getId()))).thenReturn(Optional.of(Pair.of(JOB, TASK)));
        when(jobOperations.updateTask(eq(TASK.getId()), any(), any(), anyString(), any())).thenReturn(Completable.complete());
        when(containerResultCodeResolver.resolve(any(), any())).thenReturn(Optional.empty());
    }

    @After
    public void tearDown() {
        reconcilerPodEvents.onComplete();
        reconcilerContainerEvents.onComplete();
        podEvents.onComplete();
        processor.shutdown();
    }

    @Test
    public void testOpportunisticAnnotationsArePropagatedToTasks() {
        V1Pod oldPod = new V1Pod()
                .metadata(new V1ObjectMeta().name(TASK.getId()))
                .status(new V1PodStatus()
                        .phase(PodPhase.PENDING.getPhaseName())
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(TASK.getId())
                                .state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
                        )
                );
        V1Pod updatedPod = new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(TASK.getId())
                        .putAnnotationsItem(KubeConstants.OPPORTUNISTIC_CPU_COUNT, "5")
                        .putAnnotationsItem(KubeConstants.OPPORTUNISTIC_ID, "opportunistic-resource-1234")
                )
                .spec(new V1PodSpec().nodeName("host1"))
                .status(new V1PodStatus()
                        .phase(PodPhase.PENDING.getPhaseName())
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(TASK.getId())
                                .state(new V1ContainerState().running(new V1ContainerStateRunning()))
                        )
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(TASK.getId())
                                .state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
                        )
                );
        podEvents.onNext(PodEvent.onUpdate(oldPod, updatedPod, Optional.empty()));

        verify(jobOperations, times(1)).updateTask(eq(TASK.getId()), changeFunctionCaptor.capture(), eq(V3JobOperations.Trigger.Kube),
                eq("Kube pod notification"), any());

        Function<Task, Optional<Task>> changeFunction = changeFunctionCaptor.getValue();
        assertThat(changeFunction).isNotNull();
        Optional<Task> updated = changeFunction.apply(TASK);
        assertThat(updated).isPresent();
        assertThat(updated.get().getTaskContext())
                .containsEntry(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "5")
                .containsEntry(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, "opportunistic-resource-1234");
    }

    @Test
    public void testUpdateTaskStatusVK() {
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(TASK.getId())
                )
                .status(new V1PodStatus()
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(TASK.getId())
                                .state(new V1ContainerState()
                                        .running(new V1ContainerStateRunning().startedAt(DateTime.now()))
                                )
                        )
                );
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta()
                        .annotations(CollectionsExt.asMap(
                                TITUS_NODE_DOMAIN + "ami", "ami123",
                                TITUS_NODE_DOMAIN + "stack", "myStack"
                        ))
                )
                .status(new V1NodeStatus()
                        .addresses(Collections.singletonList(
                                new V1NodeAddress().address("2.2.2.2").type(KubeUtil.TYPE_INTERNAL_IP)
                        ))
                );
        Task updatedTask = KubeNotificationProcessor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.of(new TitusExecutorDetails(Collections.emptyMap(), new TitusExecutorDetails.NetworkConfiguration(
                        true,
                        "1.2.3.4",
                        "",
                        "1.2.3.4",
                        "eniId123",
                        "resourceId123"
                ))),
                Optional.of(node),
                TASK
        );

        Set<TaskState> pastStates = updatedTask.getStatusHistory().stream().map(ExecutableStatus::getState).collect(Collectors.toSet());
        assertThat(pastStates).contains(TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated);
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "2.2.2.2");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_AMI, "ami123");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_STACK, "myStack");
    }

    @Test
    public void testUpdateTaskStatusKubelet() {
        when(containerResultCodeResolver.resolve(any(), any())).thenReturn(Optional.of("testUpdatedReasonCode"));
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(TASK.getId())
                )
                .status(new V1PodStatus()
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(TASK.getId())
                                .state(new V1ContainerState()
                                        .running(new V1ContainerStateRunning().startedAt(DateTime.now()))
                                )
                        )
                );
        pod.getStatus().setPodIP("192.0.2.0");

        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta()
                        .annotations(Collections.singletonMap(
                                TITUS_NODE_DOMAIN + "ami", "ami123"
                        ))
                )
                .status(new V1NodeStatus()
                        .addresses(Collections.singletonList(
                                new V1NodeAddress().address("2.2.2.2").type(KubeUtil.TYPE_INTERNAL_IP)
                        ))
                );
        Task updatedTask = KubeNotificationProcessor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.empty(),
                Optional.of(node),
                TASK
        );

        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "192.0.2.0");
    }

    @Test
    public void testPodPhaseFailedNoContainerCreated() {
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta().name(TASK.getId()))
                .status(new V1PodStatus().phase("Failed"));

        when(jobOperations.findTaskById(eq(TASK.getId()))).thenReturn(Optional.of(Pair.of(JOB, TASK)));
        when(jobOperations.updateTask(eq(TASK.getId()), any(), any(), anyString(), any())).thenReturn(Completable.complete());
        podEvents.onNext(PodEvent.onAdd(pod));

        verify(jobOperations, times(1)).updateTask(eq(TASK.getId()), changeFunctionCaptor.capture(), eq(V3JobOperations.Trigger.Kube),
                eq("Kube pod notification"), any());
    }

    private class FakeDirectKube implements DirectKubeApiServerIntegrator {
        @Override
        public Flux<PodEvent> events() {
            return podEvents;
        }

        @Override
        public Map<String, V1Pod> getPods() {
            throw new UnsupportedOperationException("not needed");
        }

        @Override
        public Mono<V1Pod> launchTask(Job job, Task task) {
            throw new UnsupportedOperationException("not needed");
        }

        @Override
        public Mono<Void> terminateTask(Task task) {
            throw new UnsupportedOperationException("not needed");
        }

        @Override
        public String resolveReasonCode(Throwable cause) {
            return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
        }
    }

    private class FakeReconciler implements KubeJobManagementReconciler {
        @Override
        public Flux<ContainerEvent> getV3ContainerEventSource() {
            return reconcilerContainerEvents;
        }

        @Override
        public Flux<PodEvent> getPodEventSource() {
            return reconcilerPodEvents;
        }
    }
}
