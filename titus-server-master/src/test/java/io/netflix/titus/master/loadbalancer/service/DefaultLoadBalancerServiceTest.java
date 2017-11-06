/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.loadbalancer.service;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.NoopRegistry;
import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static io.netflix.titus.master.loadbalancer.service.LoadBalancerTests.count;
import static io.netflix.titus.master.loadbalancer.service.LoadBalancerTests.matchesTarget;
import static io.netflix.titus.master.loadbalancer.service.LoadBalancerTests.mockConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerServiceTest {

    private TitusRuntime runtime;
    private LoadBalancerClient client;
    private V3JobOperations jobOperations;
    private LoadBalancerStore store;
    private TestScheduler testScheduler;

    private void defaultStubs() {
        when(client.registerAll(any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.observeJobs()).thenReturn(PublishSubject.create());
    }

    @Before
    public void setUp() throws Exception {
        runtime = new DefaultTitusRuntime(new NoopRegistry());
        client = mock(LoadBalancerClient.class);
        store = new InMemoryLoadBalancerStore();
        jobOperations = mock(V3JobOperations.class);
        testScheduler = Schedulers.test();
    }

    @Test
    public void addLoadBalancerRegistersTasks() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));

        //noinspection unchecked
        when(jobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                LoadBalancerTests.buildTasksStarted(5, jobId),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(3, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = mockConfiguration(5, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);
        verify(jobOperations).getTasks(jobId);

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == 5));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void targetsAreBufferedUpToATimeout() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));

        //noinspection unchecked
        when(jobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                LoadBalancerTests.buildTasksStarted(3, jobId),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = mockConfiguration(1000, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration,
                client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();
        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);
        verify(jobOperations).getTasks(jobId);

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

        testScheduler.advanceTimeBy(5_001, TimeUnit.MILLISECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == 3));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void addSkipLoadBalancerOperationsOnErrors() throws Exception {
        final String firstJobId = UUID.randomUUID().toString();
        final String secondJobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        defaultStubs();
        // first fails, second succeeds
        when(jobOperations.getJob(firstJobId)).thenThrow(RuntimeException.class);
        when(jobOperations.getJob(secondJobId)).thenReturn(Optional.of(Job.newBuilder().build()));
        when(jobOperations.getTasks(secondJobId)).thenReturn(LoadBalancerTests.buildTasksStarted(2, secondJobId));

        LoadBalancerConfiguration configuration = mockConfiguration(2, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        // first fails and gets skipped after being saved, so convergence can pick it up later
        assertTrue(service.addLoadBalancer(firstJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(firstJobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertNoValues();
        verify(jobOperations, never()).getTasks(firstJobId);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

        // second succeeds
        assertTrue(service.addLoadBalancer(secondJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(secondJobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(jobOperations).getTasks(secondJobId);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == 2));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void targetsAreBufferedInBatches() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int batchSize = random.nextInt(5, 20);
        final int extra = random.nextInt(1, batchSize);

        defaultStubs();
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));
        when(jobOperations.getTasks(jobId)).thenReturn(LoadBalancerTests.buildTasksStarted(batchSize + extra, jobId));

        LoadBalancerConfiguration configuration = mockConfiguration(batchSize, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void batchesWithErrorsAreSkipped() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int batchSize = random.nextInt(3, 10);
        final int extra = random.nextInt(1, batchSize);

        when(client.registerAll(any())).thenReturn(Completable.error(new RuntimeException()))
                .thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.observeJobs()).thenReturn(PublishSubject.create());
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));
        // 2 batches
        when(jobOperations.getTasks(jobId)).thenReturn(LoadBalancerTests.buildTasksStarted(2 * batchSize + extra, jobId));

        LoadBalancerConfiguration configuration = mockConfiguration(batchSize, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();

        // first errored and got skipped
        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client, times(2)).registerAll(argThat(targets -> targets != null && targets.size() == batchSize));
        verify(client, atMost(2)).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void removeLoadBalancerDeregisterKnownTargets() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);

        defaultStubs();

        assertTrue(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated)
                .await(100, TimeUnit.MILLISECONDS));
        final Map<LoadBalancerTarget, LoadBalancerTarget.State> existingTargets = CollectionsExt.<LoadBalancerTarget, LoadBalancerTarget.State>newHashMap()
                .entry(new LoadBalancerTarget(jobLoadBalancer, "Task-1", "1.1.1.1"), LoadBalancerTarget.State.Registered)
                .entry(new LoadBalancerTarget(jobLoadBalancer, "Task-2", "2.2.2.2"), LoadBalancerTarget.State.Registered)
                // should keep retrying targets that already have been marked to be deregistered
                .entry(new LoadBalancerTarget(jobLoadBalancer, "Task-3", "3.3.3.3"), LoadBalancerTarget.State.Deregistered)
                .entry(new LoadBalancerTarget(jobLoadBalancer, "Task-4", "4.4.4.4"), LoadBalancerTarget.State.Deregistered)
                .toMap();
        assertTrue(store.updateTargets(existingTargets).await(100, TimeUnit.MILLISECONDS));
        assertEquals(count(store.retrieveTargets(jobLoadBalancer)), 4);

        LoadBalancerConfiguration configuration = mockConfiguration(4, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.removeLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        final Pair<String, JobLoadBalancer.State> loadBalancer = store.retrieveLoadBalancersForJob(jobId).toBlocking().first();
        assertEquals(loadBalancer.getLeft(), loadBalancerId);
        assertEquals(loadBalancer.getRight(), JobLoadBalancer.State.Dissociated);
        assertFalse(service.getJobLoadBalancers(jobId).toBlocking().getIterator().hasNext());

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).deregisterAll(argThat(targets -> targets != null && targets.size() == 4));
        verify(client).registerAll(argThat(CollectionsExt::isNullOrEmpty));

        // all successfully deregistered are gone
        assertEquals(count(store.retrieveTargets(jobLoadBalancer)), 0);
    }

    @Test
    public void removeSkipLoadBalancerOperationsOnErrors() throws Exception {
        final String firstJobId = UUID.randomUUID().toString();
        final String secondJobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final JobLoadBalancer firstLoadBalancer = new JobLoadBalancer(firstJobId, loadBalancerId);
        final JobLoadBalancer secondLoadBalancer = new JobLoadBalancer(secondJobId, loadBalancerId);

        defaultStubs();
        store = spy(store);
        when(store.retrieveTargets(new JobLoadBalancer(firstJobId, loadBalancerId))).thenThrow(RuntimeException.class);
        assertTrue(store.addOrUpdateLoadBalancer(firstLoadBalancer, JobLoadBalancer.State.Associated)
                .await(100, TimeUnit.MILLISECONDS));
        assertTrue(store.addOrUpdateLoadBalancer(secondLoadBalancer, JobLoadBalancer.State.Associated)
                .await(100, TimeUnit.MILLISECONDS));
        Map<LoadBalancerTarget, LoadBalancerTarget.State> existingTargets = CollectionsExt.<LoadBalancerTarget, LoadBalancerTarget.State>newHashMap()
                .entry(new LoadBalancerTarget(secondLoadBalancer, "Task-1", "1.1.1.1"), LoadBalancerTarget.State.Registered)
                .entry(new LoadBalancerTarget(secondLoadBalancer, "Task-2", "2.2.2.2"), LoadBalancerTarget.State.Registered)
                .toMap();
        assertTrue(store.updateTargets(existingTargets).await(100, TimeUnit.MILLISECONDS));
        assertEquals(count(store.retrieveTargets(secondLoadBalancer)), 2);

        LoadBalancerConfiguration configuration = mockConfiguration(2, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        // first fails
        assertTrue(service.removeLoadBalancer(firstJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertFalse(service.getJobLoadBalancers(firstJobId).toBlocking().getIterator().hasNext());

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).deregisterAll(any());
        verify(client, never()).registerAll(any());
        assertEquals(count(store.retrieveTargets(secondLoadBalancer)), 2);

        // second succeeds
        assertTrue(service.removeLoadBalancer(secondJobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertFalse(service.getJobLoadBalancers(firstJobId).toBlocking().getIterator().hasNext());

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).deregisterAll(argThat(targets -> targets != null && targets.size() == 2));
        verify(client).registerAll(argThat(CollectionsExt::isNullOrEmpty));
        // all successfully deregistered are gone
        assertEquals(count(store.retrieveTargets(secondLoadBalancer)), 0);
    }

    // TODO(fabio): test JobNotFoundException from getTasks(jobId) gets ignored

    @Test
    public void newTasksGetRegistered() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String taskId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent> taskEvents = PublishSubject.create();

        when(client.registerAll(any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.observeJobs()).thenReturn(taskEvents);
        when(jobOperations.getTasks(jobId)).thenReturn(Collections.emptyList());

        LoadBalancerConfiguration configuration = mockConfiguration(1, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

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

        // events with no state transition gets ignored
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                mock(TitusModelUpdateAction.class),
                Optional.of(launched),
                Optional.empty(),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

        // events to !Started states get ignored
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(taskId),
                Optional.of(startingWithIp),
                Optional.of(launched),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

        // finally detect the task is UP and gets registered
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(taskId),
                Optional.of(started),
                Optional.of(startingWithIp),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(argThat(matchesTarget(loadBalancerId, "1.2.3.4")));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void finishedTasksGetDeregistered() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String loadBalancerId = "lb-" + UUID.randomUUID().toString();
        final PublishSubject<JobManagerEvent> taskEvents = PublishSubject.create();

        when(client.registerAll(any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.observeJobs()).thenReturn(taskEvents);
        when(jobOperations.getTasks(jobId)).thenReturn(Collections.emptyList());

        LoadBalancerConfiguration configuration = mockConfiguration(1, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(runtime, configuration, client, store, jobOperations, testScheduler);

        final AssertableSubscriber<Batch> testSubscriber = service.events().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(service.getJobLoadBalancers(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

        // a task that was prematurely killed before having an IP address associated to it should be ignored
        Task noIp = ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.KillInitiated).build())
                .build();
        Task noIpFinished = noIp.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .build();
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(noIp.getId()),
                Optional.of(noIpFinished),
                Optional.of(noIp),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(0);
        verify(client, never()).registerAll(any());
        verify(client, never()).deregisterAll(any());

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
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(first.getId()),
                Optional.of(firstFinished),
                Optional.of(first),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(client).registerAll(argThat(CollectionsExt::isNullOrEmpty));
        verify(client).deregisterAll(argThat(matchesTarget(loadBalancerId, "1.1.1.1")));

        Task second = first.toBuilder()
                .withId(UUID.randomUUID().toString())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "2.2.2.2"
                )).build();
        Task secondKilling = second.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.KillInitiated).build())
                .build();
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(second.getId()),
                Optional.of(secondKilling),
                Optional.of(second),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(2);
        verify(client, times(2)).registerAll(argThat(CollectionsExt::isNullOrEmpty));
        verify(client).deregisterAll(argThat(matchesTarget(loadBalancerId, "2.2.2.2")));

        Task third = first.toBuilder()
                .withId(UUID.randomUUID().toString())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, "3.3.3.3"
                )).build();
        Task thirdDisconnected = third.toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Disconnected).build())
                .build();
        taskEvents.onNext(new TaskUpdateEvent(
                ReconcilerEvent.EventType.ModelUpdated,
                LoadBalancerTests.mockUpdateAction(third.getId()),
                Optional.of(thirdDisconnected),
                Optional.of(third),
                Optional.empty()
        ));
        testScheduler.triggerActions();
        testSubscriber.assertNoErrors().assertValueCount(3);
        verify(client, times(3)).registerAll(argThat(CollectionsExt::isNullOrEmpty));
        verify(client).deregisterAll(argThat(matchesTarget(loadBalancerId, "3.3.3.3")));
    }
}