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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.protobuf.Empty;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetLoadBalancerResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.model.job.Container;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerValidationConfiguration;
import io.netflix.titus.api.loadbalancer.model.sanitizer.NoOpLoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.stubbing.OngoingStubbing;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalancerTests {

    private static final long TIMEOUT_MS = 30_000;

    static public LoadBalancerService getMockLoadBalancerService() {
        final TitusRuntime runtime = new DefaultTitusRuntime(new NoopRegistry());
        final LoadBalancerConfiguration loadBalancerConfig = mockConfiguration(5_000);
        final LoadBalancerConnector connector = mock(LoadBalancerConnector.class);
        final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        when(v3JobOperations.observeJobs()).thenReturn(PublishSubject.create());
        final LoadBalancerJobOperations loadBalancerJobOperations = new LoadBalancerJobOperations(v3JobOperations);
        final LoadBalancerReconciler reconciler = mock(LoadBalancerReconciler.class);
        when(reconciler.events()).thenReturn(PublishSubject.create());
        final LoadBalancerStore loadBalancerStore = new InMemoryLoadBalancerStore();
        final LoadBalancerJobValidator validator = new NoOpLoadBalancerJobValidator();
        final TestScheduler testScheduler = Schedulers.test();

        return new DefaultLoadBalancerService(runtime, loadBalancerConfig, connector, loadBalancerStore, loadBalancerJobOperations,
                reconciler, validator, testScheduler);
    }

    // Started tasks have IPs assigned to them
    static List<Task> buildTasksStarted(int count, String jobId) {
        return IntStream.range(0, count).mapToObj(i -> ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, String.format("%1$d.%1$d.%1$d.%1$d", i + 1)
                ))
                .build()
        ).collect(Collectors.toList());
    }

    static List<Task> buildTasks(int count, String jobId, TaskState state) {
        return IntStream.range(0, count).mapToObj(i -> ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(state).build())
                .build()
        ).collect(Collectors.toList());
    }

    static LoadBalancerConfiguration mockConfiguration(long minTimeInQueueMs) {
        final LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        // numbers close to Long.MAX_VALUE will trigger integer overflow bugs in the DefaultTokenBucket impl
        when(configuration.getRateLimitBurst()).thenReturn(Long.MAX_VALUE / 100);
        when(configuration.getRateLimitRefillPerSec()).thenReturn(Long.MAX_VALUE / 100);

        final LoadBalancerConfiguration.Batch batchConfig = mock(LoadBalancerConfiguration.Batch.class);
        when(configuration.getBatch()).thenReturn(batchConfig);
        when(batchConfig.getBucketSizeMs()).thenReturn(minTimeInQueueMs);
        when(batchConfig.getMaxTimeMs()).thenReturn(Long.MAX_VALUE);
        when(batchConfig.getMinTimeMs()).thenReturn(minTimeInQueueMs);

        final LoadBalancerConfiguration.Reconciliation reconciliationConfig = mock(LoadBalancerConfiguration.Reconciliation.class);
        when(configuration.getReconciliation()).thenReturn(reconciliationConfig);
        when(reconciliationConfig.getDelayMs()).thenReturn(30_000L);
        when(reconciliationConfig.getQuietPeriodMs()).thenReturn(120_000L);

        return configuration;
    }

    static LoadBalancerValidationConfiguration mockValidationConfig(int maxLbsPerJob) {
        final LoadBalancerValidationConfiguration config = mock(LoadBalancerValidationConfiguration.class);
        when(config.getMaxLoadBalancersPerJob()).thenReturn(maxLbsPerJob);
        return config;
    }

    /**
     * Common testing helper that gets load balancers for a job, ensures the gRPC request was
     * successful, and returns the load balancer ids as a set.
     */
    public static Set<LoadBalancerId> getLoadBalancersForJob(String jobIdStr,
                                                             BiConsumer<JobId, TestStreamObserver<GetLoadBalancerResult>> getJobLoadBalancers) {
        JobId jobId = JobId.newBuilder().setId(jobIdStr).build();

        TestStreamObserver<GetLoadBalancerResult> getResponse = new TestStreamObserver<>();
        getJobLoadBalancers.accept(jobId, getResponse);

        GetLoadBalancerResult result = null;
        try {
            result = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            assert false;
        }

        assertThat(getResponse.hasError()).isFalse();
        return new HashSet<>(result.getLoadBalancersList());
    }

    /**
     * Common testing helper that adds a specified number of random load balancer ids to
     * a specified number of jobs. The helper ensures the gRPC request was successful and
     * returns the job ids and load balancer ids as a map.
     */
    public static Map<String, Set<LoadBalancerId>> putLoadBalancersPerJob(int numJobs, int numLoadBalancersPerJob,
                                                                          BiConsumer<AddLoadBalancerRequest, TestStreamObserver<Empty>> putLoadBalancer) {
        // Create job entries
        Map<String, Set<LoadBalancerId>> jobIdToLoadBalancersMap = new ConcurrentHashMap<>();
        for (int i = 1; i <= numJobs; i++) {
            jobIdToLoadBalancersMap.put("Titus-" + i, new HashSet<>());
        }

        // For each job, insert load balancers
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerSet) -> {
            for (int i = 0; i < numLoadBalancersPerJob; i++) {
                LoadBalancerId loadBalancerId = LoadBalancerId.newBuilder()
                        .setId(RandomStringUtils.randomAlphanumeric(10))
                        .build();
                AddLoadBalancerRequest request = AddLoadBalancerRequest.newBuilder()
                        .setJobId(jobId)
                        .setLoadBalancerId(loadBalancerId)
                        .build();
                TestStreamObserver<Empty> addResponse = new TestStreamObserver<>();
                putLoadBalancer.accept(request, addResponse);

                assertThatCode(addResponse::awaitDone).doesNotThrowAnyException();
                assertThat(addResponse.hasError()).isFalse();

                loadBalancerSet.add(loadBalancerId);
            }
        });
        return jobIdToLoadBalancersMap;
    }

    /**
     * Common testing helper that removes a load balancer id from a job. The helper ensures the
     * gRPC request was successful.
     */
    public static void removeLoadBalancerFromJob(String jobId, LoadBalancerId loadBalancerId,
                                                 BiConsumer<RemoveLoadBalancerRequest, TestStreamObserver<Empty>> removeLoadBalancers) {
        RemoveLoadBalancerRequest request = RemoveLoadBalancerRequest.newBuilder()
                .setJobId(jobId)
                .setLoadBalancerId(loadBalancerId)
                .build();
        TestStreamObserver<Empty> removeResponse = new TestStreamObserver<>();
        removeLoadBalancers.accept(request, removeResponse);

        assertThatCode(removeResponse::awaitDone).doesNotThrowAnyException();
        assertThat(removeResponse.hasError()).isFalse();
    }

    /**
     * Configures a V3 mock to return job from getJobs() that passes validation.
     */
    static OngoingStubbing<?> applyValidGetJobMock(V3JobOperations mockedV3Ops, String jobId) {
        return when(mockedV3Ops.getJob(jobId)).thenReturn(Optional.of(Job.<ServiceJobExt>newBuilder()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .build())
                .withJobDescriptor(JobDescriptor.<ServiceJobExt>newBuilder()
                        .withExtensions(ServiceJobExt.newBuilder().build())
                        .withContainer(Container.newBuilder()
                                .withImage(Image.newBuilder().build())
                                .withContainerResources(ContainerResources.newBuilder()
                                        .withAllocateIP(true)
                                        .build())
                                .build())
                        .build())
                .build()));
    }
}
