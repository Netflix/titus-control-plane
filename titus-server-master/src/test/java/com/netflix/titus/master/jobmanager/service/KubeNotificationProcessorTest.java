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
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.NetworkConfiguration;
import com.netflix.titus.master.kubernetes.ContainerResultCodeResolver;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import com.netflix.titus.master.kubernetes.client.model.PodPhase;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.master.kubernetes.controller.KubeJobManagementReconciler;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import rx.Completable;

import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andIpAddress;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andNodeAnnotations;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.newNode;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andNodeName;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andPhase;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andPodAnnotations;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andPodIp;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andRunning;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andWaiting;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.newPod;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.TITUS_NODE_DOMAIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        processor = new KubeNotificationProcessor(new FakeDirectKube(),
                new FakeReconciler(),
                jobOperations,
                containerResultCodeResolver,
                titusRuntime
        ) {
            @Override
            protected Scheduler initializeNotificationScheduler() {
                return Schedulers.immediate();
            }
        };
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
        V1Pod oldPod = newPod(TASK.getId(), andPhase(PodPhase.PENDING.getPhaseName()), andWaiting());
        V1Pod updatedPod = newPod(TASK.getId(), andPhase(PodPhase.PENDING.getPhaseName()), andNodeName("host1"),
                andWaiting(), andRunning(),
                andPodAnnotations(KubeConstants.OPPORTUNISTIC_CPU_COUNT, "5",
                        KubeConstants.OPPORTUNISTIC_ID, "opportunistic-resource-1234")
        );
        podEvents.onNext(PodEvent.onUpdate(oldPod, updatedPod, Optional.empty()));

        verify(jobOperations, times(1)).updateTask(eq(TASK.getId()), changeFunctionCaptor.capture(), eq(V3JobOperations.Trigger.Kube),
                eq("Pod status updated from kubernetes node (k8phase='Pending', taskState=Accepted)"), any());

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
        V1Pod pod = newPod(TASK.getId(), andRunning());
        V1Node node = newNode(andIpAddress("2.2.2.2"), andNodeAnnotations(
                TITUS_NODE_DOMAIN + "ami", "ami123",
                TITUS_NODE_DOMAIN + "stack", "myStack"
        ));
        Task updatedTask = processor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.of(new TitusExecutorDetails(Collections.emptyMap(), new TitusExecutorDetails.NetworkConfiguration(
                        true,
                        "1.2.3.4",
                        "",
                        "1.2.3.4",
                        NetworkConfiguration.NetworkMode.Ipv4Only.toString(),
                        "eniId123",
                        "resourceId123"
                ))),
                Optional.of(node),
                TASK,
                false
        ).orElse(null);

        Set<TaskState> pastStates = updatedTask.getStatusHistory().stream().map(ExecutableStatus::getState).collect(Collectors.toSet());
        assertThat(pastStates).contains(TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated);
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "2.2.2.2");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_AMI, "ami123");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_STACK, "myStack");
    }

    @Test
    public void testUpdateTaskStatusVKWithTransitionNetworkMode() {
        V1Pod pod = newPod(TASK.getId(), andRunning());
        V1Node node = newNode(andIpAddress("2.2.2.2"), andNodeAnnotations(
                TITUS_NODE_DOMAIN + "ami", "ami123",
                TITUS_NODE_DOMAIN + "stack", "myStack"
        ));
        Task updatedTask = processor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.of(new TitusExecutorDetails(Collections.emptyMap(), new TitusExecutorDetails.NetworkConfiguration(
                        true,
                        "192.0.2.1",
                        "2001:db8:0:1234:0:567:8:1",
                        "192.0.2.1",
                        NetworkConfiguration.NetworkMode.Ipv6AndIpv4Fallback.toString(),
                        "eniId123",
                        "resourceId123"
                ))),
                Optional.of(node),
                TASK,
                false
        ).orElse(null);

        Set<TaskState> pastStates = updatedTask.getStatusHistory().stream().map(ExecutableStatus::getState).collect(Collectors.toSet());
        assertThat(pastStates).contains(TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated);
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "2.2.2.2");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "2001:db8:0:1234:0:567:8:1");
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV6, "2001:db8:0:1234:0:567:8:1");
        // In IPv6 + transition mode, there should *not* be a ipv4. That would be confusing because such a v4 would not
        // be unique to that task, and tools would try to use it, people would try to ssh to it, etc.
        assertThat(updatedTask.getTaskContext()).doesNotContainKey(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IPV4);
        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_TRANSITION_IPV4, "192.0.2.1");
    }

    @Test
    public void testUpdateTaskStatusKubelet() {
        when(containerResultCodeResolver.resolve(any(), any())).thenReturn(Optional.of("testUpdatedReasonCode"));
        V1Pod pod = newPod(TASK.getId(), andRunning(), andPodIp("192.0.2.0"));
        V1Node node = newNode(andIpAddress("2.2.2.2"), andNodeAnnotations(TITUS_NODE_DOMAIN + "ami", "ami123"));
        Task updatedTask = processor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.empty(),
                Optional.of(node),
                TASK,
                false
        ).orElse(null);

        assertThat(updatedTask.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "192.0.2.0");
    }

    @Test
    public void testPodPhaseFailedNoContainerCreated() {
        V1Pod pod = newPod(TASK.getId(), andPhase("Failed"));

        when(jobOperations.findTaskById(eq(TASK.getId()))).thenReturn(Optional.of(Pair.of(JOB, TASK)));
        when(jobOperations.updateTask(eq(TASK.getId()), any(), any(), anyString(), any())).thenReturn(Completable.complete());
        podEvents.onNext(PodEvent.onAdd(pod));

        verify(jobOperations, times(1)).updateTask(eq(TASK.getId()), changeFunctionCaptor.capture(), eq(V3JobOperations.Trigger.Kube),
                eq("Pod status updated from kubernetes node (k8phase='Failed', taskState=Accepted)"), any());
    }

    @Test
    public void testTaskStateDoesNotMoveBack() {
        V1Pod pod = newPod(TASK.getId(), andRunning());
        Task updatedTask = processor.updateTaskStatus(
                new PodWrapper(pod),
                TaskStatus.newBuilder().withState(TaskState.Started).build(),
                Optional.empty(),
                Optional.of(newNode()),
                JobFunctions.changeTaskStatus(TASK, TaskStatus.newBuilder().withState(TaskState.KillInitiated).build()),
                false
        ).orElse(null);
        assertThat(updatedTask).isNull();
    }

    @Test
    public void testAreTasksEquivalent_Same() {
        BatchJobTask first = JobGenerator.oneBatchTask();
        BatchJobTask second = first.toBuilder().build();
        assertThat(KubeNotificationProcessor.areTasksEquivalent(first, second)).isEmpty();
    }

    @Test
    public void testAreTasksEquivalent_DifferentStatus() {
        BatchJobTask first = JobGenerator.oneBatchTask();
        BatchJobTask second = first.toBuilder()
                .withStatus(first.getStatus().toBuilder().withReasonMessage("my important change").build())
                .build();
        assertThat(KubeNotificationProcessor.areTasksEquivalent(first, second)).contains("different task status");
    }

    @Test
    public void testAreTasksEquivalent_DifferentAttributes() {
        BatchJobTask first = JobGenerator.oneBatchTask();
        BatchJobTask second = first.toBuilder()
                .withAttributes(Collections.singletonMap("testAreTasksEquivalent_DifferentAttributes", "true"))
                .build();
        assertThat(KubeNotificationProcessor.areTasksEquivalent(first, second)).contains("different task attributes");
    }

    @Test
    public void testAreTasksEquivalent_DifferentContext() {
        BatchJobTask first = JobGenerator.oneBatchTask();
        BatchJobTask second = first.toBuilder()
                .withTaskContext(Collections.singletonMap("testAreTasksEquivalent_DifferentAttributes", "true"))
                .build();
        assertThat(KubeNotificationProcessor.areTasksEquivalent(first, second)).contains("different task context");
    }

    @Test
    public void testAreTasksEquivalent_DifferentTwoLevelResource() {
        BatchJobTask first = JobGenerator.oneBatchTask();
        BatchJobTask second = first.toBuilder()
                .withTwoLevelResources(TwoLevelResource.newBuilder().withName("fakeResource").build())
                .build();
        assertThat(KubeNotificationProcessor.areTasksEquivalent(first, second)).contains("different task two level resources");
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
        public Flux<PodEvent> getPodEventSource() {
            return reconcilerPodEvents;
        }
    }
}
