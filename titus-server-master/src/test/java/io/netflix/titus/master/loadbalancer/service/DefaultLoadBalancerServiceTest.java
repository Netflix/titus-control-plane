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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.observers.AssertableSubscriber;
import rx.plugins.RxJavaHooks;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerServiceTest {

    private LoadBalancerClient client;
    private V3JobOperations jobOperations;
    private LoadBalancerStore store;
    private TestScheduler testScheduler;

    @Before
    public void setUp() throws Exception {
        client = mock(LoadBalancerClient.class);
        store = new InMemoryLoadBalancerStore();
        jobOperations = mock(V3JobOperations.class);

        testScheduler = Schedulers.test();
        RxJavaHooks.setOnComputationScheduler(ignored -> testScheduler);
        RxJavaHooks.setOnIOScheduler(ignored -> testScheduler);
    }

    @Test
    public void addLoadBalancerRegistersTasks() throws Exception {
        String jobId = UUID.randomUUID().toString();
        String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        when(client.registerAll(any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));
        when(jobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                LoadBalancerTests.buildTasksStarted(5, jobId),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(2, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(3, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = mockConfiguration(5, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(configuration, client, store, jobOperations);

        final AssertableSubscriber<DefaultLoadBalancerService.Batch> testSubscriber = service.buildStream().test();

        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(store.retrieveLoadBalancersForJob(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.triggerActions();

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(jobOperations).getTasks(jobId);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == 5));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    @Test
    public void targetsAreBufferedUpToATimeout() throws Exception {
        String jobId = UUID.randomUUID().toString();
        String loadBalancerId = "lb-" + UUID.randomUUID().toString();

        when(client.registerAll(any())).thenReturn(Completable.complete());
        when(client.deregisterAll(any())).thenReturn(Completable.complete());
        when(jobOperations.getJob(jobId)).thenReturn(Optional.of(Job.newBuilder().build()));
        when(jobOperations.getTasks(jobId)).thenReturn(CollectionsExt.merge(
                LoadBalancerTests.buildTasksStarted(3, jobId),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.StartInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.KillInitiated),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Finished),
                LoadBalancerTests.buildTasks(1, jobId, TaskState.Disconnected)
        ));

        LoadBalancerConfiguration configuration = mockConfiguration(1000, 5_000);
        DefaultLoadBalancerService service = new DefaultLoadBalancerService(configuration, client, store, jobOperations);

        final AssertableSubscriber<DefaultLoadBalancerService.Batch> testSubscriber = service.buildStream().test();
        assertTrue(service.addLoadBalancer(jobId, loadBalancerId).await(100, TimeUnit.MILLISECONDS));
        assertThat(store.retrieveLoadBalancersForJob(jobId).toBlocking().first()).isEqualTo(loadBalancerId);

        testScheduler.advanceTimeBy(5, TimeUnit.SECONDS);

        testSubscriber.assertNoErrors().assertValueCount(1);
        verify(jobOperations).getTasks(jobId);
        verify(client).registerAll(argThat(targets -> targets != null && targets.size() == 3));
        verify(client).deregisterAll(argThat(CollectionsExt::isNullOrEmpty));
    }

    private LoadBalancerConfiguration mockConfiguration(int batchSize, long batchTimeoutMs) {
        final LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        final LoadBalancerConfiguration.Batch batchConfig = mock(LoadBalancerConfiguration.Batch.class);
        when(configuration.getBatch()).thenReturn(batchConfig);
        when(batchConfig.getSize()).thenReturn(batchSize);
        when(batchConfig.getTimeoutMs()).thenReturn(batchTimeoutMs);
        return configuration;
    }
}