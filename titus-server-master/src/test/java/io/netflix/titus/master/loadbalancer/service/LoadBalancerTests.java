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

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import rx.Observable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LoadBalancerTests {

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
}
