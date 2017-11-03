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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetLoadBalancerResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import org.apache.commons.lang3.RandomStringUtils;
import rx.Observable;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalancerTests {

    private static final long TIMEOUT_MS = 30_000;

    static public LoadBalancerService getMockLoadBalancerService() {
        final LoadBalancerConfiguration loadBalancerConfig = mockConfiguration(5, 5_000);
        final LoadBalancerClient client = mock(LoadBalancerClient.class);
        final V3JobOperations jobOperations = mock(V3JobOperations.class);
        final LoadBalancerStore store = new InMemoryLoadBalancerStore();
        final TestScheduler testScheduler = Schedulers.test();

        final DefaultLoadBalancerService loadBalancerService = new DefaultLoadBalancerService(loadBalancerConfig, client, store, jobOperations, testScheduler);
        final AssertableSubscriber<DefaultLoadBalancerService.Batch> testSubscriber = loadBalancerService.buildStream().test();

        return loadBalancerService;
    }

    // Started tasks have IPs assigned to them
    static List<Task> buildTasksStarted(int count, String jobId) {
        return IntStream.range(0, count).mapToObj(i -> ServiceJobTask.newBuilder()
                .withJobId(jobId)
                .withId(UUID.randomUUID().toString())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build())
                .withTaskContext(CollectionsExt.asMap(
                        TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, String.format("%1$d.%1$d.%1$d.%1$d", i)
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

    static LoadBalancerConfiguration mockConfiguration(int batchSize, long batchTimeoutMs) {
        final LoadBalancerConfiguration configuration = mock(LoadBalancerConfiguration.class);
        final LoadBalancerConfiguration.Batch batchConfig = mock(LoadBalancerConfiguration.Batch.class);
        when(configuration.getBatch()).thenReturn(batchConfig);
        when(batchConfig.getSize()).thenReturn(batchSize);
        when(batchConfig.getTimeoutMs()).thenReturn(batchTimeoutMs);
        return configuration;
    }

    static <T> long count(Observable<T> items) {
        return StreamSupport.<T>stream(items.toBlocking().toIterable().spliterator(), false).count();
    }

    /**
     * Common testing helper that gets load balancers for a job, ensures the gRPC request was
     * successful, and returns the load balancer ids as a set.
     * @param jobIdStr
     * @param getJobLoadBalancers
     * @return
     */
    static public Set<LoadBalancerId> getLoadBalancersForJob(String jobIdStr,
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
        return new HashSet<LoadBalancerId>(result.getLoadBalancersList());
    }

    /**
     * Common testing helper that adds a specified number of random load balancer ids to
     * a specified number of jobs. The helper ensures the gRPC request was successful and
     * returns the job ids and load balancer ids as a map.
     * @param numJobs
     * @param numLoadBalancersPerJob
     * @param putLoadBalancer
     * @return
     */
    static public Map<String, Set<LoadBalancerId>> putLoadBalancersPerJob(int numJobs, int numLoadBalancersPerJob,
                                                                          BiConsumer<AddLoadBalancerRequest, TestStreamObserver<LoadBalancerId>> putLoadBalancer) {
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
                TestStreamObserver<LoadBalancerId> addResponse = new TestStreamObserver<>();
                putLoadBalancer.accept(request, addResponse);

                LoadBalancerId loadBalancerResultId = null;
                try {
                    loadBalancerResultId = addResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    assert false;
                }
                assertThat(addResponse.hasError()).isFalse();
                assertThat(loadBalancerResultId.getId()).isNotEmpty();

                loadBalancerSet.add(loadBalancerResultId);
            }
        });
        return  jobIdToLoadBalancersMap;
    }

    /**
     * Common testing helper that removes a load balancer id from a job. The helper ensures the
     * gRPC request was successful.
     * @param jobId
     * @param loadBalancerId
     * @param removeLoadBalancers
     */
    static public void removeLoadBalancerFromJob(String jobId, LoadBalancerId loadBalancerId,
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
}
