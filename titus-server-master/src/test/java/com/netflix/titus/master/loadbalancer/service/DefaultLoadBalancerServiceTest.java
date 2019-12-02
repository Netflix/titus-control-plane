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

package com.netflix.titus.master.loadbalancer.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.connector.cloud.LoadBalancer;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.model.sanitizer.DefaultLoadBalancerJobValidator;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerValidationConfiguration;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.batch.Batch;
import com.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.Single;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State.DEREGISTERED;
import static com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State.REGISTERED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerServiceTest {

    private static final int MIN_TIME_IN_QUEUE_MS = 1_000;
    private static final int FLUSH_WAIT_TIME_MS = 2 * MIN_TIME_IN_QUEUE_MS;

    private TitusRuntime runtime;
    private LoadBalancerConnector client;
    private V3JobOperations v3JobOperations;
    private LoadBalancerJobOperations loadBalancerJobOperations;
    private LoadBalancerStore loadBalancerStore;
    private LoadBalancerReconciler reconciler;
    private PublishSubject<TargetStateBatchable> reconcilerEvents;
    private LoadBalancerJobValidator validator;
    private TestScheduler testScheduler;
    private CallMetadata callMetadata = CallMetadata.newBuilder().withCallerId("Load Balancer test").build();

    private void defaultStubs() {
        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(PublishSubject.create());
        when(client.getLoadBalancer(any())).thenAnswer(invocation -> Single.just(
                new LoadBalancer(invocation.getArgument(0), LoadBalancer.State.ACTIVE, Collections.emptySet())
        ));
    }

    @Before
    public void setUp() throws Exception {
        runtime = TitusRuntimes.internal();
        client = mock(LoadBalancerConnector.class);
        loadBalancerStore = new InMemoryLoadBalancerStore();
        reconciler = mock(LoadBalancerReconciler.class);
        reconcilerEvents = PublishSubject.create();
        when(reconciler.events()).thenReturn(reconcilerEvents.toSerialized());
        v3JobOperations = mock(V3JobOperations.class);
        loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        LoadBalancerValidationConfiguration validationConfiguration = LoadBalancerTests.mockValidationConfig(30);
        validator = new DefaultLoadBalancerJobValidator(v3JobOperations, loadBalancerStore, validationConfiguration);
        testScheduler = Schedulers.test();
    }

    @Test
    public void addLoadBalancerRegistersTasks() {
        String jobId = UUID.randomUUID().toString();
        String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        Collection<LoadBalancerTargetState> expectedTargets = tasks.stream()
                .map(task -> new LoadBalancerTargetState(
                        new LoadBalancerTarget(loadBalancerId, task.getId(), task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)),
                        REGISTERED
                ))
                .collect(Collectors.toList());

        when(v3JobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                tasks,
                LoadBalancerTests.buildTasks(2, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(3, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);
        verify(v3JobOperations).getTasks(jobId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        assertThat(loadBalancerStore.getTargets()).containsAll(expectedTargets);
        verify(client).registerAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == tasks.size()));
        verify(client, never()).deregisterAll(eq(loadBalancerId), any());
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void targetsAreBufferedUpToATimeout() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(3, jobId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                tasks,
                LoadBalancerTests.buildTasks(1, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();
        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);
        verify(v3JobOperations).getTasks(jobId);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        // targets are ignored before batching happens
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == tasks.size()));
        verify(client, never()).deregisterAll(eq(loadBalancerId), any());
    }

    @Test
    public void emptyBatchesAreFilteredOut() {
        defaultStubs();

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();
    }

    @Test
    public void addSkipLoadBalancerOperationsOnErrors() {
        final String firstJobId = UUID.randomUUID().toString();
        final String secondJobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        // first fails, second succeeds
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, firstJobId).thenThrow(new RuntimeException());
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, secondJobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(2, secondJobId);
        when(v3JobOperations.getTasks(secondJobId)).thenReturn(tasks);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        // first fails and gets skipped after being saved, so convergence can pick it up later
        assertTrue(service.addLoadBalancer(firstJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(firstJobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertNoValues();
        verify(v3JobOperations, never()).getTasks(firstJobId);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        // second succeeds
        assertTrue(service.addLoadBalancer(secondJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(secondJobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(v3JobOperations).getTasks(secondJobId);
        verify(client).registerAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == tasks.size()));
        verify(client, never()).deregisterAll(eq(loadBalancerId), any());
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void multipleLoadBalancersPerJob() {
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();
        final String jobId = UUID.randomUUID().toString();
        final String firstLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final String secondLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final int numberOfStartedTasks = 5;

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(numberOfStartedTasks, jobId);
        Collection<LoadBalancerTargetState> expectedTargets = tasks.stream()
                .flatMap(task -> Stream.of(
                        new LoadBalancerTargetState(
                                new LoadBalancerTarget(firstLoadBalancerId, task.getId(), task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)),
                                REGISTERED
                        ),
                        new LoadBalancerTargetState(
                                new LoadBalancerTarget(secondLoadBalancerId, task.getId(), task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)),
                                REGISTERED
                        ))
                )
                .collect(Collectors.toList());

        when(v3JobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                tasks,
                LoadBalancerTests.buildTasks(2, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(3, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        // associate two load balancers to the same job

        assertTrue(service.addLoadBalancer(jobId, firstLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertTrue(service.addLoadBalancer(jobId, secondLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toList().toBlocking().single())
                .containsOnly(firstLoadBalancerId, secondLoadBalancerId);
        verify(v3JobOperations, times(2)).getTasks(jobId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        // 1 batch per loadbalancer
        testSubscriber.assertNoErrors().assertValueCount(2);
        assertThat(loadBalancerStore.getTargets()).containsAll(expectedTargets);
        verify(client).registerAll(eq(firstLoadBalancerId), argThat(targets -> targets != null && targets.size() == numberOfStartedTasks));
        verify(client).registerAll(eq(secondLoadBalancerId), argThat(targets -> targets != null && targets.size() == numberOfStartedTasks));
        verify(client, never()).deregisterAll(eq(firstLoadBalancerId), any());
        verify(client, never()).deregisterAll(eq(secondLoadBalancerId), any());
        verifyReconcilerIgnore(firstLoadBalancerId, LoadBalancerTests.ipAddresses(tasks));
        verifyReconcilerIgnore(secondLoadBalancerId, LoadBalancerTests.ipAddresses(tasks));

        // now some more tasks are added to the job, check if both load balancers get updated

        List<Task> newTasks = new ArrayList<>();
        for (int i = 1; i <= numberOfStartedTasks; i++) {
            final String taskId = UUID.randomUUID().toString();
            final Task startingWithIp = ServiceJobTask.newBuilder()
                    .withJobId(jobId)
                    .withId(taskId)
                    .withStatus(TaskStatus.newBuilder().withState(TaskState.StartInitiated).build())
                    .withTaskContext(CollectionsExt.asMap(
                            TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, String.format("%1$d.%1$d.%1$d.%1$d", i + numberOfStartedTasks)
                    )).build();
            final Task started = startingWithIp.toBuilder()
                    .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                    .build();
            newTasks.add(started);

            taskEvents.onNext(TaskUpdateEvent.taskChange(null, started, startingWithIp, callMetadata));
        }

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        // 2 more batches (one per load balancer)
        testSubscriber.assertNoErrors().assertValueCount(4);
        verify(client, times(2)).registerAll(eq(firstLoadBalancerId), argThat(targets -> targets != null && targets.size() == numberOfStartedTasks));
        verify(client, times(2)).registerAll(eq(secondLoadBalancerId), argThat(targets -> targets != null && targets.size() == numberOfStartedTasks));
        verify(client, never()).deregisterAll(eq(firstLoadBalancerId), any());
        verify(client, never()).deregisterAll(eq(secondLoadBalancerId), any());
        verifyReconcilerIgnore(firstLoadBalancerId, LoadBalancerTests.ipAddresses(newTasks));
        verifyReconcilerIgnore(secondLoadBalancerId, LoadBalancerTests.ipAddresses(newTasks));
    }

    @Test
    public void targetsAreBufferedInBatches() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int batchSize = random.nextInt(5, 20);

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(batchSize, jobId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client, never()).deregisterAll(eq(loadBalancerId), any());
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void batchesWithErrorsAreSkipped() {
        final String jobId = UUID.randomUUID().toString();
        final String firstLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final String secondLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int batchSize = random.nextInt(3, 10);

        when(client.registerAll(eq(firstLoadBalancerId), any())).thenReturn(Completable.error(new RuntimeException()));
        when(client.registerAll(eq(secondLoadBalancerId), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(PublishSubject.create());
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(batchSize, jobId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, firstLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertTrue(service.addLoadBalancer(jobId, secondLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().toIterable())
                .containsExactlyInAnyOrder(firstLoadBalancerId, secondLoadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        // first errored and got skipped
        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(eq(firstLoadBalancerId), argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client).registerAll(eq(secondLoadBalancerId), argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client, never()).deregisterAll(any(), any());
        // we still ignore reconciliation because the failure happens later in the connector
        verifyReconcilerIgnore(firstLoadBalancerId, LoadBalancerTests.ipAddresses(tasks));
        verifyReconcilerIgnore(secondLoadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void reconciliationErrorsDontHaltOthers() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int batchSize = random.nextInt(3, 10);

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(batchSize, jobId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        reconcilerEvents.onError(new RuntimeException("first error"));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors();

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
        reconcilerEvents.onError(new RuntimeException("second error"));

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client, never()).deregisterAll(any(), any());
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void removeLoadBalancerDeregisterKnownTargets() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        Collection<LoadBalancerTargetState> expectedTargets = tasks.stream()
                .map(task -> new LoadBalancerTargetState(
                        new LoadBalancerTarget(loadBalancerId, task.getId(), task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)),
                        DEREGISTERED
                ))
                .collect(Collectors.toList());
        when(v3JobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                tasks,
                LoadBalancerTests.buildTasks(2, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(3, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .await(100, TimeUnit.MILLISECONDS));
        assertTrue(service.removeLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        List<JobLoadBalancerState> jobLoadBalancers = loadBalancerStore.getAssociations().stream()
                .filter(association -> jobId.equals(association.getJobId()))
                .collect(Collectors.toList());
        assertThat(jobLoadBalancers).isNotEmpty();
        assertThat(jobLoadBalancers).hasSize(1);
        JobLoadBalancerState jobLoadBalancerState = jobLoadBalancers.iterator().next();
        assertEquals(loadBalancerId, jobLoadBalancerState.getLoadBalancerId());
        assertEquals(JobLoadBalancer.State.DISSOCIATED, jobLoadBalancerState.getState());
        assertFalse(service.getJobLoadBalancers(jobId).toBlocking().getIterator().hasNext());

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        assertThat(loadBalancerStore.getTargets()).containsAll(expectedTargets);
        verify(client, never()).registerAll(eq(loadBalancerId), any());
        verify(client).deregisterAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == tasks.size()));
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));

    }

    @Test
    public void removeSkipLoadBalancerOperationsOnErrors() {
        final String firstJobId = UUID.randomUUID().toString();
        final String secondJobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final JobLoadBalancer firstLoadBalancer = new JobLoadBalancer(firstJobId, loadBalancerId);
        final JobLoadBalancer secondLoadBalancer = new JobLoadBalancer(secondJobId, loadBalancerId);

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, firstJobId);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, secondJobId);
        when(v3JobOperations.getTasks(firstJobId)).thenThrow(new RuntimeException());
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, secondJobId);
        when(v3JobOperations.getTasks(secondJobId)).thenReturn(tasks);
        assertTrue(loadBalancerStore.addOrUpdateLoadBalancer(firstLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .await(100, TimeUnit.MILLISECONDS));
        assertTrue(loadBalancerStore.addOrUpdateLoadBalancer(secondLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .await(100, TimeUnit.MILLISECONDS));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        // first fails
        assertTrue(service.removeLoadBalancer(firstJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertFalse(service.getJobLoadBalancers(firstJobId).toBlocking().getIterator().hasNext());

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).deregisterAll(any(), any());
        verify(client, never()).registerAll(any(), any());
        verifyNoReconcilerIgnore();

        // second succeeds
        assertTrue(service.removeLoadBalancer(secondJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertFalse(service.getJobLoadBalancers(firstJobId).toBlocking().getIterator().hasNext());

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client, never()).registerAll(eq(loadBalancerId), any());
        verify(client).deregisterAll(eq(loadBalancerId), argThat(targets -> targets != null && targets.size() == tasks.size()));
        verifyReconcilerIgnore(loadBalancerId, LoadBalancerTests.ipAddresses(tasks));
    }

    @Test
    public void goneJobsAreSkipped() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);
        // job is gone somewhere in the middle after its pipeline starts
        when(v3JobOperations.getTasks(jobId)).thenThrow(JobManagerException.jobNotFound(jobId));

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        // job errored and got skipped
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();
    }

    @Test
    public void newTasksGetRegistered() {
        final String jobId = UUID.randomUUID().toString();
        final String taskId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        when(v3JobOperations.getTasks(jobId)).thenReturn(Collections.emptyList());
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        Task launched = ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(taskId)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Launched).build())
                .build();

        Task startingWithIp = launched.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.StartInitiated).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4"
                )).build();

        Task started = startingWithIp.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .build();
        LoadBalancerTargetState expectedTarget = new LoadBalancerTargetState(
                new LoadBalancerTarget(loadBalancerId, started.getId(), "1.2.3.4"),
                REGISTERED
        );

        // events with no state transition gets ignored
        taskEvents.onNext(TaskUpdateEvent.newTask(null, launched, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        // events to !Started states get ignored
        taskEvents.onNext(TaskUpdateEvent.taskChange(null, startingWithIp, launched, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        // finally detect the task is UP and gets registered
        taskEvents.onNext(TaskUpdateEvent.taskChange(null, started, startingWithIp, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        assertThat(loadBalancerStore.getTargets()).contains(expectedTarget);
        verify(client).registerAll(eq(loadBalancerId), argThat(set -> set.contains("1.2.3.4")));
        verify(client, never()).deregisterAll(eq(loadBalancerId), any());
        verifyReconcilerIgnore(loadBalancerId, "1.2.3.4");
    }

    @Test
    public void finishedTasksGetDeregistered() {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        when(v3JobOperations.getTasks(jobId)).thenReturn(Collections.emptyList());
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, jobId);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        // a task that was prematurely killed before having an IP address associated to it should be ignored
        Task noIp = ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.KillInitiated).build())
                .build();
        Task noIpFinished = noIp.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .build();

        taskEvents.onNext(TaskUpdateEvent.taskChange(null, noIpFinished, noIp, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        // 3 state transitions to 3 different terminal events

        Task first = noIp.toBuilder()
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.1.1.1"
                )).build();
        Task firstFinished = first.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .build();
        LoadBalancerTargetState expectedFirstTarget = new LoadBalancerTargetState(
                new LoadBalancerTarget(loadBalancerId, firstFinished.getId(), "1.1.1.1"),
                DEREGISTERED
        );
        assertThat(loadBalancerStore.getTargets()).doesNotContain(expectedFirstTarget);

        taskEvents.onNext(TaskUpdateEvent.taskChange(null, firstFinished, first, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        assertThat(loadBalancerStore.getTargets()).contains(expectedFirstTarget);
        verify(client, never()).registerAll(eq(loadBalancerId), any());
        verify(client).deregisterAll(eq(loadBalancerId), argThat(set -> set.contains("1.1.1.1")));
        verifyReconcilerIgnore(loadBalancerId, "1.1.1.1");

        Task second = first.toBuilder()
                .withId(UUID.randomUUID().toString())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "2.2.2.2"
                )).build();
        Task secondKilling = second.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.KillInitiated).build())
                .build();
        LoadBalancerTargetState expectedSecondTarget = new LoadBalancerTargetState(
                new LoadBalancerTarget(loadBalancerId, secondKilling.getId(), "2.2.2.2"),
                DEREGISTERED
        );
        assertThat(loadBalancerStore.getTargets()).doesNotContain(expectedSecondTarget);

        taskEvents.onNext(TaskUpdateEvent.taskChange(null, secondKilling, second, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(2);
        assertThat(loadBalancerStore.getTargets()).contains(expectedSecondTarget);
        verify(client, never()).registerAll(eq(loadBalancerId), any());
        verify(client).deregisterAll(eq(loadBalancerId), argThat(set -> set.contains("2.2.2.2")));
        verifyReconcilerIgnore(loadBalancerId, "2.2.2.2");

        Task third = first.toBuilder()
                .withId(UUID.randomUUID().toString())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "3.3.3.3"
                )).build();
        Task thirdDisconnected = third.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Disconnected).build())
                .build();
        LoadBalancerTargetState expectedThirdTarget = new LoadBalancerTargetState(
                new LoadBalancerTarget(loadBalancerId, thirdDisconnected.getId(), "3.3.3.3"),
                DEREGISTERED
        );
        assertThat(loadBalancerStore.getTargets()).doesNotContain(expectedThirdTarget);

        taskEvents.onNext(TaskUpdateEvent.taskChange(null, thirdDisconnected, third, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(3);
        assertThat(loadBalancerStore.getTargets()).contains(expectedThirdTarget);
        verify(client, never()).registerAll(eq(loadBalancerId), any());
        verify(client).deregisterAll(eq(loadBalancerId), argThat(set -> set.contains("3.3.3.3")));
        verifyReconcilerIgnore(loadBalancerId, "3.3.3.3");
    }

    @Test
    public void movedTasks() {
        final String taskId = UUID.randomUUID().toString();
        final String sourceJobId = UUID.randomUUID().toString();
        final String targetJobId = UUID.randomUUID().toString();
        final String sourceLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final String targetLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final String commonLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, sourceJobId);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, targetJobId);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(sourceJobId, sourceLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertTrue(service.addLoadBalancer(sourceJobId, commonLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(sourceJobId).toBlocking().toIterable())
                .containsExactlyInAnyOrder(sourceLoadBalancerId, commonLoadBalancerId);

        assertTrue(service.addLoadBalancer(targetJobId, targetLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertTrue(service.addLoadBalancer(targetJobId, commonLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(targetJobId).toBlocking().toIterable())
                .containsExactlyInAnyOrder(targetLoadBalancerId, commonLoadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        Task moved = ServiceJobTask.newBuilder()
                .withJobId(targetJobId)
                .withId(taskId)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4",
                        TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, sourceJobId
                )).build();

        // detect the task is moved, gets deregistered from the source and registered on the target
        taskEvents.onNext(TaskUpdateEvent.newTaskFromAnotherJob(null, moved, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(2);
        verify(client).registerAll(eq(targetLoadBalancerId), argThat(set -> set.contains("1.2.3.4")));
        verify(client).deregisterAll(eq(sourceLoadBalancerId), argThat(set -> set.contains("1.2.3.4")));
        verifyReconcilerIgnore(targetLoadBalancerId, "1.2.3.4");
        verifyReconcilerIgnore(sourceLoadBalancerId, "1.2.3.4");

        // load balancers associated with both source and target jobs are not changed
        verify(client, never()).registerAll(eq(commonLoadBalancerId), any());
        verify(client, never()).deregisterAll(eq(commonLoadBalancerId), any());
        verifyNoReconcilerIgnore(commonLoadBalancerId);
        verifyNoReconcilerIgnore(commonLoadBalancerId);
    }

    @Test
    public void movedTaskOnlyTargetAssociatedWithLoadBalancer() {
        final String taskId = UUID.randomUUID().toString();
        final String sourceJobId = UUID.randomUUID().toString();
        final String targetJobId = UUID.randomUUID().toString();
        final String targetLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, sourceJobId);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, targetJobId);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertThat(service.getJobLoadBalancers(sourceJobId).toBlocking().toIterable()).isEmpty();

        assertTrue(service.addLoadBalancer(targetJobId, targetLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(targetJobId).toBlocking().toIterable())
                .containsExactlyInAnyOrder(targetLoadBalancerId);

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        Task moved = ServiceJobTask.newBuilder()
                .withJobId(targetJobId)
                .withId(taskId)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4",
                        TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, sourceJobId
                )).build();

        // detect the task is moved and gets registered on the target
        taskEvents.onNext(TaskUpdateEvent.newTaskFromAnotherJob(null, moved, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(eq(targetLoadBalancerId), argThat(set -> set.contains("1.2.3.4")));
        verify(client, never()).deregisterAll(any(), any());
        verifyReconcilerIgnore(targetLoadBalancerId, "1.2.3.4");
    }

    @Test
    public void movedTaskOnlySourceAssociatedWithLoadBalancer() {
        final String taskId = UUID.randomUUID().toString();
        final String sourceJobId = UUID.randomUUID().toString();
        final String targetJobId = UUID.randomUUID().toString();
        final String sourceLoadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent<?>> taskEvents = PublishSubject.create();

        when(client.registerAll(any(), any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any(), any())).thenReturn(Completable.complete());
        when(v3JobOperations.observeJobs()).thenReturn(taskEvents);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, sourceJobId);
        LoadBalancerTests.applyValidGetJobMock(v3JobOperations, targetJobId);

        LoadBalancerConfiguration configuration = LoadBalancerTests.mockConfiguration(MIN_TIME_IN_QUEUE_MS);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(
                runtime, configuration, client, loadBalancerStore, loadBalancerJobOperations, reconciler, validator, testScheduler);

        final AssertableSubscriber<Batch<TargetStateBatchable, String>> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(sourceJobId, sourceLoadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(sourceJobId).toBlocking().toIterable())
                .containsExactlyInAnyOrder(sourceLoadBalancerId);

        assertThat(service.getJobLoadBalancers(targetJobId).toBlocking().toIterable()).isEmpty();

        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any(), any());
        verify(client, never()).deregisterAll(any(), any());
        verifyNoReconcilerIgnore();

        Task moved = ServiceJobTask.newBuilder()
                .withJobId(targetJobId)
                .withId(taskId)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "1.2.3.4",
                        TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, sourceJobId
                )).build();

        // detect the task is moved and gets deregistered from the source
        taskEvents.onNext(TaskUpdateEvent.newTaskFromAnotherJob(null, moved, callMetadata));
        testScheduler.advanceTimeBy(FLUSH_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).deregisterAll(eq(sourceLoadBalancerId), argThat(set -> set.contains("1.2.3.4")));
        verify(client, never()).registerAll(any(), any());
        verifyReconcilerIgnore(sourceLoadBalancerId, "1.2.3.4");
    }

    private void verifyReconcilerIgnore(String loadBalancerId, String... ipAddresses) {
        final Set<String> ipSet = CollectionsExt.asSet(ipAddresses);
        verify(reconciler, times(ipAddresses.length)).activateCooldownFor(
                argThat(target -> loadBalancerId.equals(target.getLoadBalancerId())
                        && ipSet.contains(target.getIpAddress())),
                anyLong(),
                any()
        );
    }

    private void verifyNoReconcilerIgnore() {
        verify(reconciler, never()).activateCooldownFor(any(), anyLong(), any());
    }

    private void verifyNoReconcilerIgnore(String loadBalancerId) {
        verify(reconciler, never()).activateCooldownFor(
                argThat(target -> loadBalancerId.equals(target.getLoadBalancerId())),
                anyLong(),
                any()
        );
    }

}
