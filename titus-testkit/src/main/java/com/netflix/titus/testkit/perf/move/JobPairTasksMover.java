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

package com.netflix.titus.testkit.perf.move;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Single;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toCoreJob;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toCoreTask;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;

class JobPairTasksMover {

    private static final Logger logger = LoggerFactory.getLogger(JobPairTasksMover.class);

    private final JobServiceGateway client;
    private final String jobId1;
    private final String jobId2;
    private final int jobSize;
    private final int batchSize;

    private volatile Job<ServiceJobExt> job1;
    private volatile Job<ServiceJobExt> job2;
    private final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<>();

    JobPairTasksMover(JobServiceGateway client, String jobId1, String jobId2, int jobSize, int batchSize) {
        this.client = client;
        this.jobId1 = jobId1;
        this.jobId2 = jobId2;
        this.jobSize = jobSize;
        this.batchSize = batchSize;
        logger.info("Created new job pair: job1={}, job2={}", jobId1, jobId2);
    }

    Flux<Void> waitForTasksToStart() {
        Observable<Void> jobObservable = client.observeJob(jobId1, JobManagerConstants.UNDEFINED_CALL_METADATA)
                .doOnNext(event -> {
                    switch (event.getNotificationCase()) {
                        case JOBUPDATE:
                            job1 = toCoreJob(event.getJobUpdate().getJob());
                            break;
                        case TASKUPDATE:
                            Task task = toCoreTask(job1, event.getTaskUpdate().getTask());
                            if (task.getStatus().getState() == TaskState.Started) {
                                tasks.put(task.getId(), task);
                                logger.info("Task started: jobId={}, startTaskId={}, remaining={}", jobId1, task.getId(), jobSize - tasks.size());
                            }
                            break;
                    }
                })
                .takeUntil(event -> tasks.size() == jobSize)
                .ignoreElements()
                .cast(Void.class);

        return ReactorExt.toFlux(jobObservable);
    }

    Flux<Void> move() {
        logger.info("Moving tasks: job1={}, job2={}...", jobId1, jobId2);

        List<Completable> moveActions = tasks.keySet().stream()
                .map(taskId -> client.moveTask(
                        TaskMoveRequest.newBuilder().setTaskId(taskId).setTargetJobId(jobId2).build(),
                        JobManagerConstants.UNDEFINED_CALL_METADATA
                ))
                .collect(Collectors.toList());

        return ReactorExt.toFlux(Completable.merge(Observable.from(moveActions), batchSize).toObservable().cast(Void.class))
                .doOnTerminate(() -> logger.info("Moved tasks: job1={}, job2={}...", jobId1, jobId2));
    }

    void shutdown() {
        Throwable error = Completable.merge(
                client.killJob(jobId1, JobManagerConstants.UNDEFINED_CALL_METADATA),
                client.killJob(jobId2, JobManagerConstants.UNDEFINED_CALL_METADATA)
        ).get();
        if (error != null) {
            logger.warn("Job cleanup error for jobs: job1={}, job2={}", jobId1, jobId2);
        } else {
            logger.info("Removed jobs: job1={}, job2={}", jobId1, jobId2);
        }
    }

    static Mono<JobPairTasksMover> newTaskMover(JobServiceGateway client, JobDescriptor<ServiceJobExt> jobDescriptor, int jobSize, int batchSize) {
        JobDescriptor<ServiceJobExt> first = JobFunctions.changeServiceJobCapacity(
                jobDescriptor,
                Capacity.newBuilder()
                        .withMin(0)
                        .withDesired(jobSize)
                        .withMax(jobSize)
                        .build()
        );

        JobDescriptor<ServiceJobExt> second = JobFunctions.changeServiceJobCapacity(
                jobDescriptor,
                Capacity.newBuilder()
                        .withMin(0)
                        .withDesired(0)
                        .withMax(jobSize)
                        .build()
        );

        Single<JobPairTasksMover> action = Observable.zip(
                client.createJob(toGrpcJobDescriptor(first), JobManagerConstants.UNDEFINED_CALL_METADATA),
                client.createJob(toGrpcJobDescriptor(second), JobManagerConstants.UNDEFINED_CALL_METADATA),
                (j1, j2) -> new JobPairTasksMover(client, j1, j2, jobSize, batchSize)
        ).take(1).toSingle();

        return ReactorExt.toMono(action);
    }
}
