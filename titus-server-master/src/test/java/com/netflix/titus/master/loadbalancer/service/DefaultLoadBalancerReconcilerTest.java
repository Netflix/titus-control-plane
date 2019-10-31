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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.LoadBalancer;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.batch.Priority;
import com.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import rx.Single;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerReconcilerTest {

    private String loadBalancerId;
    private String jobId;
    private long delayMs;
    private LoadBalancerConfiguration configuration;
    private LoadBalancerStore store;
    private LoadBalancerConnector connector;
    private V3JobOperations v3JobOperations;
    private LoadBalancerJobOperations loadBalancerJobOperations;
    private Registry registry;
    private TestScheduler testScheduler;
    private LoadBalancerReconciler reconciler;

    @Before
    public void setUp() throws Exception {
        loadBalancerId = UUID.randomUUID().toString();
        jobId = UUID.randomUUID().toString();
        delayMs = 60_000L;/* 1 min */
        configuration = mockConfigWithDelay(delayMs);
        store = mock(LoadBalancerStore.class);
        connector = mock(LoadBalancerConnector.class);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.ACTIVE, Collections.emptySet())
        ));
        v3JobOperations = mock(V3JobOperations.class);
        loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        registry = new NoopRegistry();
        testScheduler = Schedulers.test();
        reconciler = buildReconciler(store);
    }

    private LoadBalancerReconciler buildReconciler(LoadBalancerStore store) {
        return new DefaultLoadBalancerReconciler(configuration, store, connector, loadBalancerJobOperations,
                registry, testScheduler);
    }

    @Test
    public void registerMissingTargets() {
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.Registered);
            // reconciliation always generates Priority.Low events that can be replaced by higher priority reactive updates
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test
    public void deregisterExtraTargets() {
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(3, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        reset(connector);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(new LoadBalancer(
                loadBalancerId,
                LoadBalancer.State.ACTIVE,
                CollectionsExt.asSet("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5")
        )));
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(2);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.Deregistered);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
            assertThat(update.getIdentifier().getTaskId()).isEqualTo("UNKNOWN-TASK");
        });
    }

    @Test
    public void updatesAreIgnoredWhileCooldownIsActive() {
        final long cooldownPeriodMs = 5 * delayMs;

        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        for (Task task : tasks) {
            final String ipAddress = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
            final LoadBalancerTarget target = new LoadBalancerTarget(loadBalancerId, task.getId(), ipAddress);
            reconciler.activateCooldownFor(target, cooldownPeriodMs, TimeUnit.MILLISECONDS);
        }

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(4 * delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.Registered);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });

        // try again since it still can't see updates applied on the connector
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(10);
    }

    @Test
    public void jobsWithErrorsAreIgnored() {
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);
        when(v3JobOperations.getTasks(jobId))
                .thenThrow(JobManagerException.class) // first fails
                .thenReturn(tasks);
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.Registered);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test
    public void connectorErrorsDoNotHaltReconciliation() {
        final String failingLoadBalancerId = UUID.randomUUID().toString();
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);
        final JobLoadBalancer failingJobLoadBalancer = new JobLoadBalancer(jobId, failingLoadBalancerId);
        final JobLoadBalancerState failingAssociation = new JobLoadBalancerState(failingJobLoadBalancer, JobLoadBalancer.State.Associated);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(connector.getLoadBalancer(failingLoadBalancerId)).thenReturn(Single.error(new RuntimeException("rate limit")));
        when(store.getAssociations()).thenReturn(Arrays.asList(failingAssociation, association));

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNoErrors().assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        // failingLoadBalancerId gets ignored
        subscriber.assertNoErrors().assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.Registered);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test
    public void orphanJobAssociationsAreSetAsDissociatedAndRemoved() {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenThrow(JobManagerException.jobNotFound(jobId));
        when(v3JobOperations.getJob(jobId)).thenReturn(Optional.empty());
        store = new InMemoryLoadBalancerStore();
        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated)
                .await(5, TimeUnit.SECONDS)).isTrue();

        reconciler = buildReconciler(store);
        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        // first phase mark
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        // second phase sweep
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        assertThat(store.getAssociations()).isEmpty();
        assertThat(store.getAssociatedLoadBalancersSetForJob(jobId)).isEmpty();
    }

    @Test
    public void orphanLoadBalancerAssociationsAreSetAsDissociatedAndRemoved() {
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        reset(connector);
        OngoingStubbing<Single<LoadBalancer>> ongoingStubbing = when(connector.getLoadBalancer(loadBalancerId))
                .thenReturn(Single.just(new LoadBalancer(
                        loadBalancerId,
                        LoadBalancer.State.ACTIVE,
                        CollectionsExt.asSet("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5")
                )));

        store = new InMemoryLoadBalancerStore();
        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated)
                .await(5, TimeUnit.SECONDS)).isTrue();

        reconciler = buildReconciler(store);

        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        // load balancer was removed outside of Titus
        ongoingStubbing.thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.REMOVED, Collections.emptySet())
        ));

        // first phase mark as orphan
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        // second phase update orphan as Dissociated
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        // third phase sweep all Dissociated
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        assertThat(store.getAssociations()).isEmpty();
        assertThat(store.getAssociatedLoadBalancersSetForJob(jobId)).isEmpty();
    }

    @Test
    public void dissociatedJobsAreNotRemovedUntilAllTargetsAreDeregistered() {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenThrow(JobManagerException.jobNotFound(jobId));
        when(v3JobOperations.getJob(jobId)).thenReturn(Optional.empty());
        reset(connector);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.ACTIVE, Collections.singleton("1.2.3.4"))
        ));
        store = new InMemoryLoadBalancerStore();
        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Dissociated)
                .await(5, TimeUnit.SECONDS)).isTrue();

        reconciler = buildReconciler(store);
        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertValueCount(1);
        assertThat(store.getAssociations()).isNotEmpty().hasSize(1);

        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.ACTIVE, Collections.emptySet())
        ));
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();

        assertThat(store.getAssociations()).isEmpty();
    }

    private LoadBalancerConfiguration mockConfigWithDelay(long delayMs) {
        final LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        when(configuration.getReconciliationDelayMs()).thenReturn(delayMs);
        return configuration;
    }
}
