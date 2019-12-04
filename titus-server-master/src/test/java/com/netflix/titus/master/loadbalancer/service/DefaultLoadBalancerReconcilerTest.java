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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.batch.Priority;
import com.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Single;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerReconcilerTest {

    private static final long TEST_TIMEOUT_MS = 10_0000;

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
        store = new InMemoryLoadBalancerStore();
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

    @Test(timeout = TEST_TIMEOUT_MS)
    public void registerMissingTargets() {
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState()).await();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.REGISTERED);
            // reconciliation always generates Priority.Low events that can be replaced by higher priority reactive updates
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void deregisterExtraTargetsPreviouslyRegisteredByUs() {
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(3, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        reset(connector);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(new LoadBalancer(
                loadBalancerId,
                LoadBalancer.State.ACTIVE,
                CollectionsExt.asSet("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5", "6.6.6.6")
        )));
        store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState()).await();
        Mono.when(
                // 3 running tasks were previously registered by us and are in the load balancer
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, tasks.get(0).getId(), "1.1.1.1"), LoadBalancerTarget.State.REGISTERED),
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, tasks.get(1).getId(), "2.2.2.2"), LoadBalancerTarget.State.REGISTERED),
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, tasks.get(2).getId(), "3.3.3.3"), LoadBalancerTarget.State.REGISTERED),
                // Next two ips were previously registered by us, but their tasks do not exist anymore
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, "some-dead-task", "4.4.4.4"), LoadBalancerTarget.State.REGISTERED),
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, "another-dead-task", "5.5.5.5"), LoadBalancerTarget.State.DEREGISTERED)
                // no record for 6.6.6.6, that ip address was not registered by us, and won't be touched
        ).block();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(2);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.DEREGISTERED);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
            assertThat(update.getIdentifier().getTaskId()).isIn("some-dead-task", "another-dead-task");
            assertThat(update.getIdentifier().getIpAddress()).isIn("4.4.4.4", "5.5.5.5");
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void updatesAreIgnoredWhileCooldownIsActive() {
        long cooldownPeriodMs = 5 * delayMs;
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState()).await();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        for (Task task : tasks) {
            String ipAddress = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
            LoadBalancerTarget target = new LoadBalancerTarget(loadBalancerId, task.getId(), ipAddress);
            reconciler.activateCooldownFor(target, cooldownPeriodMs, TimeUnit.MILLISECONDS);
        }

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(4 * delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.REGISTERED);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });

        // try again since it still can't see updates applied on the connector
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(10);
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void jobsWithErrorsAreIgnored() {
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId))
                .thenThrow(JobManagerException.class) // first fails
                .thenReturn(tasks);
        store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState()).await();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.REGISTERED);
            assertThat(update.getPriority()).isEqualTo(Priority.LOW);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void connectorErrorsDoNotHaltReconciliation() {
        String failingLoadBalancerId = UUID.randomUUID().toString();
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        JobLoadBalancer failingJobLoadBalancer = new JobLoadBalancer(jobId, failingLoadBalancerId);
        JobLoadBalancerState failingAssociation = new JobLoadBalancerState(failingJobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(connector.getLoadBalancer(failingLoadBalancerId)).thenReturn(Single.error(new RuntimeException("rate limit")));
        Completable.merge(
                store.addOrUpdateLoadBalancer(failingAssociation.getJobLoadBalancer(), failingAssociation.getState()),
                store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState())
        ).await();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNoErrors().assertNotCompleted().assertNoValues();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        // failingLoadBalancerId gets ignored
        subscriber.assertNoErrors().assertNotCompleted().assertValueCount(5);
        subscriber.getOnNextEvents().forEach(update -> {
            assertThat(update.getState()).isEqualTo(LoadBalancerTarget.State.REGISTERED);
            assertThat(update.getPriority()).isEqualTo(Priority.Low);
            assertThat(update.getLoadBalancerId()).isEqualTo(loadBalancerId);
        });
    }

    @Test
    public void orphanJobAssociationsAreSetAsDissociatedAndRemoved() {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenThrow(JobManagerException.jobNotFound(jobId));
        when(v3JobOperations.getJob(jobId)).thenReturn(Optional.empty());
        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .await(5, TimeUnit.SECONDS)).isTrue();

        reconciler = buildReconciler(store);
        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

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
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        reset(connector);
        OngoingStubbing<Single<LoadBalancer>> ongoingStubbing = when(connector.getLoadBalancer(loadBalancerId))
                .thenReturn(Single.just(new LoadBalancer(
                        loadBalancerId,
                        LoadBalancer.State.ACTIVE,
                        CollectionsExt.asSet("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5")
                )));

        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .await(5, TimeUnit.SECONDS)).isTrue();

        // all targets were previously registered by us
        Mono.when(tasks.stream()
                .map(task -> store.addOrUpdateTarget(
                        new LoadBalancerTarget(loadBalancerId, task.getId(),
                                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)),
                        LoadBalancerTarget.State.REGISTERED)
                )
                .collect(Collectors.toList())
        ).block();

        reconciler = buildReconciler(store);

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        // load balancer was removed outside of Titus
        ongoingStubbing.thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.REMOVED, Collections.emptySet())
        ));

        // 1. mark as orphan
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        // 2. update orphan as Dissociated
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();

        // 3. sweep all targets
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block()).isEmpty();

        // 4. sweep all Dissociated
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertNoValues();
        assertThat(store.getAssociations()).isEmpty();
        assertThat(store.getAssociatedLoadBalancersSetForJob(jobId)).isEmpty();
    }

    @Test
    public void dissociatedJobsAreNotRemovedUntilAllTargetsAreDeregisteredAndRemoved() {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        when(v3JobOperations.getTasks(jobId)).thenThrow(JobManagerException.jobNotFound(jobId));
        when(v3JobOperations.getJob(jobId)).thenReturn(Optional.empty());
        reset(connector);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.ACTIVE, Collections.singleton("1.2.3.4"))
        ));
        store.addOrUpdateTarget(
                new LoadBalancerTarget(loadBalancerId, "some-task", "1.2.3.4"),
                LoadBalancerTarget.State.DEREGISTERED
        ).block();
        assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.DISSOCIATED)
                .await(5, TimeUnit.SECONDS)).isTrue();

        reconciler = buildReconciler(store);
        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();

        // 1. deregister
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent().assertValueCount(1);
        assertThat(store.getAssociations()).isNotEmpty().hasSize(1);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(
                new LoadBalancer(loadBalancerId, LoadBalancer.State.ACTIVE, Collections.emptySet())
        ));

        // 2. clean up target state
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();
        assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block()).isEmpty();

        // 3. clean up association
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();
        assertThat(store.getAssociations()).isEmpty();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void deregisteredTargetsAreCleanedUp() {
        List<Task> tasks = LoadBalancerTests.buildTasksStarted(1, jobId);
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED);
        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        reset(connector);
        when(connector.getLoadBalancer(loadBalancerId)).thenReturn(Single.just(new LoadBalancer(
                loadBalancerId,
                LoadBalancer.State.ACTIVE,
                CollectionsExt.asSet("1.1.1.1", "10.10.10.10")
        )));
        store.addOrUpdateLoadBalancer(association.getJobLoadBalancer(), association.getState()).await();
        Mono.when(
                // running tasks was previously registered by us and are in the load balancer
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, tasks.get(0).getId(), "1.1.1.1"), LoadBalancerTarget.State.REGISTERED),
                // Next three ips were previously registered by us, but their tasks do not exist anymore and are not in the load balancer anymore
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, "target-inconsistent", "2.2.2.2"), LoadBalancerTarget.State.REGISTERED),
                store.addOrUpdateTarget(new LoadBalancerTarget(loadBalancerId, "target-not-in-lb", "3.3.3.3"), LoadBalancerTarget.State.DEREGISTERED)
                // no record for 10.10.10.10, that ip address was not registered by us, and won't be touched
        ).block();

        AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        // no reconciliation ran yet
        testScheduler.triggerActions();
        subscriber.assertNotCompleted().assertNoValues();
        assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block()).hasSize(3);

        // first pass, the one stored as DEREGISTERED is cleaned up, the other in an inconsistent state is fixed
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(1);
        TargetStateBatchable inconsistencyFix = subscriber.getOnNextEvents().get(0);
        assertThat(inconsistencyFix.getState()).isEqualTo(LoadBalancerTarget.State.DEREGISTERED);
        assertThat(inconsistencyFix.getLoadBalancerId()).isEqualTo(loadBalancerId);
        assertThat(inconsistencyFix.getIpAddress()).isEqualTo("2.2.2.2");
        List<LoadBalancerTargetState> storedTargets = store.getLoadBalancerTargets(loadBalancerId).collectList().block();
        assertThat(storedTargets).hasSize(2);
        assertThat(storedTargets).doesNotContain(new LoadBalancerTargetState(
                new LoadBalancerTarget(loadBalancerId, "target-not-in-lb", "3.3.3.3"),
                LoadBalancerTarget.State.DEREGISTERED
        ));

        // update with fix not applied yet, keep trying
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(2);
        TargetStateBatchable update2 = subscriber.getOnNextEvents().get(0);
        assertThat(update2.getState()).isEqualTo(LoadBalancerTarget.State.DEREGISTERED);
        assertThat(update2.getLoadBalancerId()).isEqualTo(loadBalancerId);
        assertThat(update2.getIpAddress()).isEqualTo("2.2.2.2");
        assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block()).hasSize(2);

        // simulate the update with the fix above being applied
        store.addOrUpdateTarget(
                new LoadBalancerTarget(loadBalancerId, "target-inconsistent", "2.2.2.2"),
                LoadBalancerTarget.State.DEREGISTERED
        ).block();

        // finally, corrected record is now cleaned up
        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        subscriber.assertNotCompleted().assertValueCount(2); // no changes needed
        assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block())
                .containsOnly(new LoadBalancerTargetState(
                        new LoadBalancerTarget(loadBalancerId, tasks.get(0).getId(), "1.1.1.1"),
                        LoadBalancerTarget.State.REGISTERED
                ));
    }

    private LoadBalancerConfiguration mockConfigWithDelay(long delayMs) {
        LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        when(configuration.getReconciliationDelayMs()).thenReturn(delayMs);
        return configuration;
    }
}
