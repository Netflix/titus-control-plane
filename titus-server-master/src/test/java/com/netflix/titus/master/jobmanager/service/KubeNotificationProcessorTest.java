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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.kubeapiserver.KubeConstants;
import com.netflix.titus.master.mesos.kubeapiserver.KubeJobManagementReconciler;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KubeNotificationProcessorTest {
    private DirectProcessor<PodEvent> podEvents;
    private DirectProcessor<PodEvent> reconcilerPodEvents;
    private DirectProcessor<ContainerEvent> reconcilerContainerEvents;
    private KubeNotificationProcessor processor;

    @Mock
    private V3JobOperations jobOperations;
    @Captor
    private ArgumentCaptor<Function<Task, Optional<Task>>> changeFunctionCaptor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        podEvents = DirectProcessor.create();
        reconcilerPodEvents = DirectProcessor.create();
        reconcilerContainerEvents = DirectProcessor.create();
        processor = new KubeNotificationProcessor(mock(JobManagerConfiguration.class), new FakeDirectKube(), new FakeReconciler(), jobOperations);
        processor.enterActiveMode();
    }

    @After
    public void tearDown() {
        reconcilerPodEvents.onComplete();
        reconcilerContainerEvents.onComplete();
        podEvents.onComplete();
        processor.shutdown();
    }

    @Test
    public void opportunisticAnnotationsArePropagatedToTasks() {
        String jobId = UUID.randomUUID().toString();
        String taskId = UUID.randomUUID().toString();
        Job<JobDescriptor.JobDescriptorExt> job = Job.newBuilder().withId(jobId).build();
        BatchJobTask task = BatchJobTask.newBuilder()
                .withId(taskId)
                .withTaskContext(CollectionsExt.asMap(TaskAttributes.TASK_ATTRIBUTES_OWNED_BY_KUBE_SCHEDULER, "true"))
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .build();
        when(jobOperations.findTaskById(eq(taskId))).thenReturn(Optional.of(Pair.of(job, task)));
        when(jobOperations.updateTask(eq(taskId), any(), any(), anyString(), any())).thenReturn(Completable.complete());

        V1Pod oldPod = new V1Pod()
                .metadata(new V1ObjectMeta().name(taskId))
                .status(new V1PodStatus()
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(taskId)
                                .state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
                        )
                );
        V1Pod updatedPod = new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(taskId)
                        .putAnnotationsItem(KubeConstants.OPPORTUNISTIC_CPU_COUNT, "5")
                        .putAnnotationsItem(KubeConstants.OPPORTUNISTIC_ID, "opportunistic-resource-1234")
                )
                .status(new V1PodStatus()
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(taskId)
                                .state(new V1ContainerState().running(new V1ContainerStateRunning()))
                        )
                        .addContainerStatusesItem(new V1ContainerStatus()
                                .containerID(taskId)
                                .state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
                        )
                );
        podEvents.onNext(PodEvent.onUpdate(oldPod, updatedPod, Optional.empty()));

        verify(jobOperations, after(5000)).updateTask(eq(taskId), changeFunctionCaptor.capture(), eq(V3JobOperations.Trigger.Kube),
                eq("Kube pod notification"), any());

        Function<Task, Optional<Task>> changeFunction = changeFunctionCaptor.getValue();
        assertThat(changeFunction).isNotNull();
        Optional<Task> updated = changeFunction.apply(task);
        assertThat(updated).isPresent();
        assertThat(updated.get().getTaskContext())
                .containsEntry(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "5")
                .containsEntry(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, "opportunistic-resource-1234");
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
