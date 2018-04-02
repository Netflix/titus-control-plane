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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.common.util.ProtobufCopy;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private static final Set<String> JOB_MINIMUM_FIELD_SET = asSet("id");
    private static final Set<String> TASK_MINIMUM_FIELD_SET = asSet("id");

    private final TitusServiceGateway<String, JobDescriptor, JobSpecCase, Job, Task, TaskStatus.TaskState> serviceGateway;
    private final SessionContext sessionContext;

    @Inject
    public DefaultJobManagementServiceGrpc(TitusServiceGateway<String, JobDescriptor, JobSpecCase, Job, Task, TaskStatus.TaskState> serviceGateway,
                                           SessionContext sessionContext) {
        this.serviceGateway = serviceGateway;
        this.sessionContext = sessionContext;
    }

    @Override
    public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
        execute(responseObserver, userId ->
                serviceGateway.createJob(
                        request
                ).subscribe(
                        jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                        e -> safeOnError(logger, e, responseObserver),
                        responseObserver::onCompleted
                ));
    }

    @Override
    public void findJobs(JobQuery jobQuery, StreamObserver<JobQueryResult> responseObserver) {
        if (!checkPageIsValid(jobQuery.getPage(), responseObserver)) {
            return;
        }
        try {
            JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> criteria = toJobQueryCriteria(jobQuery);
            Pair<List<Job>, Pagination> queryResult = serviceGateway.findJobsByCriteria(
                    criteria, Optional.of(toPage(jobQuery.getPage()))
            );

            if (!jobQuery.getFieldsList().isEmpty()) {
                Set<String> fields = new HashSet<>(jobQuery.getFieldsList());
                fields.addAll(JOB_MINIMUM_FIELD_SET);
                queryResult = queryResult.mapLeft(jobs -> jobs.stream().map(j -> ProtobufCopy.copy(j, fields)).collect(Collectors.toList()));
            }

            responseObserver.onNext(toJobQueryResult(queryResult.getLeft(), queryResult.getRight()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        String id = request.getId();

        serviceGateway.findJobById(id, true, Collections.emptySet()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
    }

    @Override
    public void findTasks(TaskQuery taskQuery, StreamObserver<TaskQueryResult> responseObserver) {
        if (!checkPageIsValid(taskQuery.getPage(), responseObserver)) {
            return;
        }
        try {
            JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> criteria = toJobQueryCriteria(taskQuery);
            Pair<List<Task>, Pagination> queryResult = serviceGateway.findTasksByCriteria(
                    criteria, Optional.of(toPage(taskQuery.getPage()))
            );

            if (!taskQuery.getFieldsList().isEmpty()) {
                Set<String> fields = new HashSet<>(taskQuery.getFieldsList());
                fields.addAll(TASK_MINIMUM_FIELD_SET);
                queryResult = queryResult.mapLeft(tasks -> tasks.stream().map(t -> ProtobufCopy.copy(t, fields)).collect(Collectors.toList()));
            }

            responseObserver.onNext(toTaskQueryResult(queryResult.getLeft(), queryResult.getRight()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        String id = request.getId();

        serviceGateway.findTaskById(id).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        execute(responseObserver, userId -> {
            Capacity taskInstances = request.getCapacity();
            serviceGateway.resizeJob(
                    userId, request.getJobId(), taskInstances.getDesired(), taskInstances.getMin(), taskInstances.getMax()
            ).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        execute(responseObserver, userId -> {
            ServiceJobSpec.ServiceJobProcesses serviceJobProcesses = request.getServiceJobProcesses();
            serviceGateway.updateJobProcesses(
                    userId, request.getJobId(), serviceJobProcesses.getDisableDecreaseDesired(), serviceJobProcesses.getDisableIncreaseDesired()
            ).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        execute(responseObserver, userId ->
                serviceGateway.changeJobInServiceStatus(
                        userId, request.getId(), request.getEnableStatus()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void killJob(JobId request, StreamObserver<Empty> responseObserver) {
        execute(responseObserver, userId ->
                serviceGateway.killJob(
                        userId, request.getId()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        // TODO shrink?
        execute(responseObserver, userId ->
                serviceGateway.killTask(
                        userId, request.getTaskId(), request.getShrink()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
        Observable<JobChangeNotification> eventStream = serviceGateway.observeJobs();
        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("All jobs monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        String id = request.getId();
        Observable<JobChangeNotification> eventStream = serviceGateway.observeJob(id);
        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription(id + " job monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    /**
     * Helper class working as key selector for building distinct stream of Job info objects.
     * Currently we observe only number of workers and their state.
     */
    private void execute(StreamObserver<?> responseObserver, Consumer<String> action) {
        if (!sessionContext.getCallerId().isPresent()) {
            responseObserver.onError(TitusServiceException.noCallerId());
            return;
        }
        try {
            action.accept(sessionContext.getCallerId().get());
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private JobQueryResult toJobQueryResult(List<Job> jobs, Pagination runtimePagination) {
        return JobQueryResult.newBuilder()
                .addAllItems(jobs)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }

    private TaskQueryResult toTaskQueryResult(List<Task> tasks, Pagination runtimePagination) {
        return TaskQueryResult.newBuilder()
                .addAllItems(tasks)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }
}
