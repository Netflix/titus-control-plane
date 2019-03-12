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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private final JobManagementClient jobManagementClient;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultJobManagementServiceGrpc(JobManagementClient jobManagementClient,
                                           SystemLogService systemLog,
                                           CallMetadataResolver callMetadataResolver) {
        this.jobManagementClient = jobManagementClient;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
        CallMetadata callMetadata = callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
        
        Subscription subscription = jobManagementClient.createJob(request, callMetadata).subscribe(
                jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.updateJobCapacity(request), responseObserver);
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.updateJobProcesses(request), responseObserver);
    }

    @Override
    public void findJobs(JobQuery jobQuery, StreamObserver<JobQueryResult> responseObserver) {
        if (!checkPageIsValid(jobQuery.getPage(), responseObserver)) {
            return;
        }
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findJobs", jobQuery.getPage());
        Subscription subscription = jobManagementClient.findJobs(jobQuery).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        Subscription subscription = jobManagementClient.findJob(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.updateJobStatus(request), responseObserver);
    }

    @Override
    public void updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, StreamObserver<Empty> responseObserver) {
        streamMonoResponse(jobManagementClient.updateJobDisruptionBudget(request), responseObserver);
    }

    @Override
    public void updateJobAttributes(JobAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        streamMonoResponse(jobManagementClient.updateJobAttributes(request), responseObserver);
    }

    @Override
    public void deleteJobAttributes(JobAttributesDeleteRequest request, StreamObserver<Empty> responseObserver) {
        streamMonoResponse(jobManagementClient.deleteJobAttributes(request), responseObserver);
    }

    @Override
    public void observeJobs(ObserveJobsQuery request, StreamObserver<JobChangeNotification> responseObserver) {
        Subscription subscription = jobManagementClient.observeJobs(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        Subscription subscription = jobManagementClient.observeJob(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void killJob(JobId request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.killJob(request.getId()), responseObserver);
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        Subscription subscription = jobManagementClient.findTask(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findTasks(TaskQuery request, StreamObserver<TaskQueryResult> responseObserver) {
        if (!checkPageIsValid(request.getPage(), responseObserver)) {
            return;
        }
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findTasks", request.getPage());
        Subscription subscription = jobManagementClient.findTasks(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.killTask(request), responseObserver);
    }

    @Override
    public void updateTaskAttributes(TaskAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.updateTaskAttributes(request), responseObserver);
    }

    @Override
    public void deleteTaskAttributes(TaskAttributesDeleteRequest request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.deleteTaskAttributes(request), responseObserver);
    }

    @Override
    public void moveTask(TaskMoveRequest request, StreamObserver<Empty> responseObserver) {
        streamCompletableResponse(jobManagementClient.moveTask(request), responseObserver);
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

    private static void streamMonoResponse(Mono<Void> completable, StreamObserver<Empty> responseObserver) {
        Disposable subscription = completable.subscribe(
                next -> {
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                }
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
