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

package com.netflix.titus.gateway.endpoint.v3.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.runtime.service.JobManagementService;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Subscription;

import static com.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private final JobManagementService jobManagementService;

    @Inject
    public DefaultJobManagementServiceGrpc(JobManagementService jobManagementService) {
        this.jobManagementService = jobManagementService;
    }

    @Override
    public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
        Subscription subscription = jobManagementService.createJob(request).subscribe(
                jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementService.updateJobCapacity(request), responseObserver);
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementService.updateJobProcesses(request), responseObserver);
    }

    @Override
    public void findJobs(JobQuery jobQuery, StreamObserver<JobQueryResult> responseObserver) {
        Subscription subscription = jobManagementService.findJobs(jobQuery).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        Subscription subscription = jobManagementService.findJob(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementService.updateJobStatus(request), responseObserver);
    }

    @Override
    public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
        Subscription subscription = jobManagementService.observeJobs().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        Subscription subscription = jobManagementService.observeJob(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void killJob(JobId request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementService.killJob(request.getId()), responseObserver);
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        Subscription subscription = jobManagementService.findTask(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findTasks(TaskQuery request, StreamObserver<TaskQueryResult> responseObserver) {
        Subscription subscription = jobManagementService.findTasks(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementService.killTask(request), responseObserver);
    }

    private static void streamCompletableResponse(Completable completable, StreamObserver<Empty> responseObserver) {
        Subscription subscription = completable.subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);

    }
}
