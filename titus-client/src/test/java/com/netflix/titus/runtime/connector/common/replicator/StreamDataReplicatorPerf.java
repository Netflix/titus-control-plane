/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.common.replicator;

import java.time.Duration;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshotFactory;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

/**
 * Test to expose memory leaks.
 */
public class StreamDataReplicatorPerf {

    private static final Job<?> JOB = JobGenerator.oneBatchJob();
    private static final Task TASK = JobGenerator.oneBatchTask().toBuilder().withJobId(JOB.getId()).build();

    public static void main(String[] args) throws InterruptedException {
        JobManagementClient client = Mockito.mock(JobManagementClient.class);

        Mockito.when(client.observeJobs(ArgumentMatchers.any())).thenAnswer(invocation -> Flux.defer(() -> {

            JobManagerEvent jobUpdateEvent = JobUpdateEvent.newJob(JOB, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
            JobManagerEvent taskUpdateEvent = TaskUpdateEvent.newTask(JOB, TASK, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);

            return Flux.just(jobUpdateEvent, JobManagerEvent.snapshotMarker())
                    .concatWith(Flux.interval(Duration.ofSeconds(1)).take(1).map(tick -> taskUpdateEvent))
                    .concatWith(Flux.interval(Duration.ofSeconds(1)).take(1).flatMap(tick -> Flux.error(new RuntimeException("Simulated error"))));
        }));

        JobDataReplicator replicator = new JobDataReplicatorProvider(client, JobSnapshotFactory.newDefault(), TitusRuntimes.internal()).get();
        replicator.events().subscribe(System.out::println);

        Thread.sleep(3600_000);
    }
}
