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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.batch.Priority;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.junit.Test;
import rx.Single;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerReconcilerTest {

    @Test
    public void registerMissingTargets() {
        final long delayMs = 60_000L /* 1 min */;
        final LoadBalancerConfiguration configuration = mockConfigWithDelay(delayMs);
        final LoadBalancerStore store = mock(LoadBalancerStore.class);
        final LoadBalancerConnector connector = mock(LoadBalancerConnector.class);
        final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        final LoadBalancerJobOperations loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        final TestScheduler testScheduler = Schedulers.test();

        final String loadBalancerId = UUID.randomUUID().toString();
        final String jobId = UUID.randomUUID().toString();
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);

        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(connector.getRegisteredIps(loadBalancerId)).thenReturn(Single.just(Collections.emptySet()));
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final LoadBalancerReconciler reconciler = new DefaultLoadBalancerReconciler(configuration, store, connector, loadBalancerJobOperations, testScheduler);
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
        final long delayMs = 60_000L /* 1 min */;
        final LoadBalancerConfiguration configuration = mockConfigWithDelay(delayMs);
        final LoadBalancerStore store = mock(LoadBalancerStore.class);
        final LoadBalancerConnector connector = mock(LoadBalancerConnector.class);
        final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        final LoadBalancerJobOperations loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        final TestScheduler testScheduler = Schedulers.test();

        final String loadBalancerId = UUID.randomUUID().toString();
        final String jobId = UUID.randomUUID().toString();
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(3, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);

        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(connector.getRegisteredIps(loadBalancerId)).thenReturn(Single.just(CollectionsExt.asSet(
                "1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"
        )));
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final LoadBalancerReconciler reconciler = new DefaultLoadBalancerReconciler(configuration, store, connector, loadBalancerJobOperations, testScheduler);
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
            assertThat(update.getIdentifier().getJobId()).isEqualTo("UNKNOWN-JOB");
        });
    }

    @Test
    public void ignoreEventsTemporarily() {
        final long delayMs = 60_000L /* 1 min */;
        final long quietPeriodMs = 5 * delayMs;

        final LoadBalancerConfiguration configuration = mockConfigWithDelay(delayMs);
        final LoadBalancerStore store = mock(LoadBalancerStore.class);
        final LoadBalancerConnector connector = mock(LoadBalancerConnector.class);
        final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        final LoadBalancerJobOperations loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        final TestScheduler testScheduler = Schedulers.test();

        final String loadBalancerId = UUID.randomUUID().toString();
        final String jobId = UUID.randomUUID().toString();
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);

        when(v3JobOperations.getTasks(jobId)).thenReturn(tasks);
        when(connector.getRegisteredIps(loadBalancerId)).thenReturn(Single.just(Collections.emptySet()));
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final LoadBalancerReconciler reconciler = new DefaultLoadBalancerReconciler(configuration, store, connector, loadBalancerJobOperations, testScheduler);
        final AssertableSubscriber<TargetStateBatchable> subscriber = reconciler.events().test();

        for (Task task : tasks) {
            final String ipAddress = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
            final LoadBalancerTarget target = new LoadBalancerTarget(jobLoadBalancer, task.getId(), ipAddress);
            reconciler.ignoreEventsFor(target, quietPeriodMs, TimeUnit.MILLISECONDS);
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
        final long delayMs = 60_000L /* 1 min */;
        final LoadBalancerConfiguration configuration = mockConfigWithDelay(delayMs);
        final LoadBalancerStore store = mock(LoadBalancerStore.class);
        final LoadBalancerConnector connector = mock(LoadBalancerConnector.class);
        final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        final LoadBalancerJobOperations loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        final TestScheduler testScheduler = Schedulers.test();

        final String loadBalancerId = UUID.randomUUID().toString();
        final String jobId = UUID.randomUUID().toString();
        final List<Task> tasks = LoadBalancerTests.buildTasksStarted(5, jobId);
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        final JobLoadBalancerState association = new JobLoadBalancerState(jobLoadBalancer, JobLoadBalancer.State.Associated);

        when(v3JobOperations.getTasks(jobId))
                .thenThrow(JobManagerException.class) // first fails
                .thenReturn(tasks);
        when(connector.getRegisteredIps(loadBalancerId)).thenReturn(Single.just(Collections.emptySet()));
        when(store.getAssociations()).thenReturn(Collections.singletonList(association));

        final LoadBalancerReconciler reconciler = new DefaultLoadBalancerReconciler(configuration, store, connector, loadBalancerJobOperations, testScheduler);
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

    private LoadBalancerConfiguration mockConfigWithDelay(long delayMs) {
        final LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        when(configuration.getReconciliationDelayMs()).thenReturn(delayMs);
        return configuration;
    }
}