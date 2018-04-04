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

package com.netflix.titus.federation.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.grpc.EmitterWithMultipleSubscriptions;
import com.netflix.titus.common.grpc.GrpcUtil;
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.concurrency.CallbackCountDownLatch;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.TaskUpdate;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import com.netflix.titus.runtime.service.JobManagementService;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_STACK;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_STACK;
import static com.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.federation.service.PageAggregationUtil.combinePagination;
import static com.netflix.titus.federation.service.PageAggregationUtil.takeCombinedPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.emptyGrpcPagination;

@Singleton
public class AggregatingJobManagementService implements JobManagementService {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingJobManagementService.class);
    private final GrpcConfiguration grpcConfiguration;
    private final TitusFederationConfiguration federationConfiguration;
    private final CellConnector connector;
    private final AllCells allCells;
    private final CellRouter router;
    private final SessionContext sessionContext;

    @Inject
    public AggregatingJobManagementService(GrpcConfiguration grpcConfiguration,
                                           TitusFederationConfiguration federationConfiguration,
                                           CellConnector connector,
                                           CellRouter router,
                                           SessionContext sessionContext) {
        this.grpcConfiguration = grpcConfiguration;
        this.federationConfiguration = federationConfiguration;
        this.connector = connector;
        this.allCells = new AllCells(connector);
        this.router = router;
        this.sessionContext = sessionContext;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        String routeKey = CellRouterUtil.getRouteKeyFromJob(jobDescriptor);
        Cell cell = router.routeKey(routeKey);
        logger.debug("Routing JobDescriptor {} to Cell {} with key {}", jobDescriptor, cell, routeKey);

        Optional<JobManagementServiceStub> optionalClient = CellConnectorUtil.toStub(cell, connector, JobManagementServiceGrpc::newStub);
        if (!optionalClient.isPresent()) {
            return Observable.error(TitusServiceException.cellNotFound(routeKey));
        }
        JobManagementServiceStub client = wrap(optionalClient.get());

        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            client.createJob(jobDescriptor, streamObserver);
        }, grpcConfiguration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate request) {
        Observable<Empty> result = findJobInAllCells(request.getJobId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, streamObserver) -> wrap(client).updateJobCapacity(request, streamObserver))
                );
        return result.toCompletable();
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate request) {
        Observable<Empty> result = findJobInAllCells(request.getJobId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, streamObserver) -> wrap(client).updateJobProcesses(request, streamObserver))
                );
        return result.toCompletable();
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate request) {
        Observable<Empty> result = findJobInAllCells(request.getId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, streamObserver) -> wrap(client).updateJobStatus(request, streamObserver))
                );
        return result.toCompletable();
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        return findJobInAllCells(jobId).map(CellResponse::getResult);
    }

    private Observable<CellResponse<JobManagementServiceStub, Job>> findJobInAllCells(String jobId) {
        return allCells.callExpectingErrors(JobManagementServiceGrpc::newStub, findJobInCell(jobId))
                .reduce(ResponseMerger.singleValue(pointQueriesErrorMerger()))
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    private SingleCellCall<Job> findJobInCell(String jobId) {
        JobId id = JobId.newBuilder().setId(jobId).build();
        return (client, streamObserver) -> wrap(client).findJob(id, streamObserver);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery request) {
        if (request.getPage().getPageSize() <= 0) {
            return Observable.just(JobQueryResult.newBuilder()
                    .setPagination(emptyGrpcPagination(request.getPage()))
                    .build());
        }
        if (StringExt.isNotEmpty(request.getPage().getCursor()) || request.getPage().getPageNumber() == 0) {
            return findJobsWithCursorPagination(request);
        }
        // TODO: page number pagination
        return Observable.error(TitusServiceException.invalidArgument("pageNumbers are not supported, please use cursors"));
    }

    private Observable<JobQueryResult> findJobsWithCursorPagination(JobQuery request) {
        return allCells.call(JobManagementServiceGrpc::newStub, findJobsInCell(request))
                .map(CellResponse::getResult)
                .reduce(AggregatingJobManagementService::combineJobResults)
                .map(combinedResults -> {
                    Pair<List<Job>, Pagination> combinedPage = takeCombinedPage(
                            request.getPage(),
                            combinedResults.getItemsList(),
                            combinedResults.getPagination(),
                            this::addStackName,
                            JobManagerCursors.jobCursorOrderComparator(),
                            JobManagerCursors::newCursorFrom
                    );

                    return JobQueryResult.newBuilder()
                            .addAllItems(combinedPage.getLeft())
                            .setPagination(combinedPage.getRight())
                            .build();
                });
    }

    private SingleCellCall<JobQueryResult> findJobsInCell(JobQuery request) {
        return (client, streamObserver) -> wrap(client).findJobs(request, streamObserver);
    }

    private static JobQueryResult combineJobResults(JobQueryResult one, JobQueryResult other) {
        Pagination pagination = combinePagination(one.getPagination(), other.getPagination());
        return JobQueryResult.newBuilder()
                .setPagination(pagination)
                .addAllItems(one.getItemsList())
                .addAllItems(other.getItemsList())
                .build();
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return Observable.error(TitusServiceException.unimplemented());
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        final Observable<JobChangeNotification> observable = createRequestObservable(delegate -> {
            Emitter<JobChangeNotification> emitter = new EmitterWithMultipleSubscriptions<>(delegate);
            Map<Cell, JobManagementServiceStub> clients = CellConnectorUtil.stubs(connector, JobManagementServiceGrpc::newStub);
            final CountDownLatch markersEmitted = new CallbackCountDownLatch(clients.size(),
                    () -> emitter.onNext(buildJobSnapshotEndMarker())
            );
            clients.forEach((cell, client) -> {
                StreamObserver<JobChangeNotification> streamObserver = new FilterOutFirstMarker(emitter, markersEmitted);
                wrap(client).observeJobs(Empty.getDefaultInstance(), streamObserver);
            });
        });
        return observable.map(this::addStackName);
    }

    @Override
    public Completable killJob(String jobId) {
        JobId id = JobId.newBuilder().setId(jobId).build();
        Observable<Empty> result = findJobInAllCells(jobId)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, streamObserver) -> wrap(client).killJob(id, streamObserver))
                );
        return result.toCompletable();
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        return findTaskInAllCells(taskId).map(CellResponse::getResult);
    }

    private Observable<CellResponse<JobManagementServiceStub, Task>> findTaskInAllCells(String taskId) {
        return allCells.callExpectingErrors(JobManagementServiceGrpc::newStub, findTaskInCell(taskId))
                .reduce(ResponseMerger.singleValue(pointQueriesErrorMerger()))
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    private SingleCellCall<Task> findTaskInCell(String taskId) {
        TaskId id = TaskId.newBuilder().setId(taskId).build();
        return (client, streamObserver) -> wrap(client).findTask(id, streamObserver);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery request) {
        if (request.getPage().getPageSize() <= 0) {
            return Observable.just(TaskQueryResult.newBuilder()
                    .setPagination(emptyGrpcPagination(request.getPage()))
                    .build());
        }
        if (StringExt.isNotEmpty(request.getPage().getCursor()) || request.getPage().getPageNumber() == 0) {
            return findTasksWithCursorPagination(request);
        }
        // TODO: page number pagination
        return Observable.error(TitusServiceException.invalidArgument("pageNumbers are not supported, please use cursors"));
    }

    private Observable<TaskQueryResult> findTasksWithCursorPagination(TaskQuery request) {
        return allCells.call(JobManagementServiceGrpc::newStub, findTasksInCell(request))
                .map(CellResponse::getResult)
                .reduce(AggregatingJobManagementService::combineTaskResults)
                .map(combinedResults -> {
                    Pair<List<Task>, Pagination> combinedPage = takeCombinedPage(
                            request.getPage(),
                            combinedResults.getItemsList(),
                            combinedResults.getPagination(),
                            this::addStackName,
                            JobManagerCursors.taskCursorOrderComparator(),
                            JobManagerCursors::newCursorFrom
                    );
                    return TaskQueryResult.newBuilder()
                            .addAllItems(combinedPage.getLeft())
                            .setPagination(combinedPage.getRight())
                            .build();
                });
    }

    private SingleCellCall<TaskQueryResult> findTasksInCell(TaskQuery request) {
        return (client, streamObserver) -> wrap(client).findTasks(request, streamObserver);
    }

    private static TaskQueryResult combineTaskResults(TaskQueryResult one, TaskQueryResult other) {
        Pagination pagination = combinePagination(one.getPagination(), other.getPagination());
        return TaskQueryResult.newBuilder()
                .setPagination(pagination)
                .addAllItems(one.getItemsList())
                .addAllItems(other.getItemsList())
                .build();
    }

    @Override
    public Completable killTask(TaskKillRequest request) {
        Observable<Empty> result = findTaskInAllCells(request.getTaskId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, streamObserver) -> wrap(client).killTask(request, streamObserver))
                );
        return result.toCompletable();
    }

    private Job addStackName(Job job) {
        JobDescriptor jobDescriptor = job.getJobDescriptor().toBuilder()
                .putAttributes(JOB_ATTRIBUTES_STACK, federationConfiguration.getStack())
                .build();
        return job.toBuilder().setJobDescriptor(jobDescriptor).build();
    }

    private Task addStackName(Task task) {
        return task.toBuilder()
                .putTaskContext(TASK_ATTRIBUTES_STACK, federationConfiguration.getStack())
                .build();
    }

    private JobChangeNotification addStackName(JobChangeNotification notification) {
        switch (notification.getNotificationCase()) {
            case JOBUPDATE:
                Job job = addStackName(notification.getJobUpdate().getJob());
                JobUpdate jobUpdate = notification.getJobUpdate().toBuilder().setJob(job).build();
                return notification.toBuilder().setJobUpdate(jobUpdate).build();
            case TASKUPDATE:
                Task task = addStackName(notification.getTaskUpdate().getTask());
                TaskUpdate taskUpdate = notification.getTaskUpdate().toBuilder().setTask(task).build();
                return notification.toBuilder().setTaskUpdate(taskUpdate).build();
            default:
                return notification;
        }
    }

    private static JobChangeNotification buildJobSnapshotEndMarker() {
        final JobChangeNotification.SnapshotEnd marker = JobChangeNotification.SnapshotEnd.newBuilder().build();
        return JobChangeNotification.newBuilder().setSnapshotEnd(marker).build();
    }

    private JobManagementServiceStub wrap(JobManagementServiceStub client) {
        return createWrappedStub(client, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    private <T> Observable<T> singleCellCall(Cell cell, SingleCellCall<T> singleCellCall) {
        return CellConnectorUtil.callToCell(cell, connector, JobManagementServiceGrpc::newStub, singleCellCall);
    }

    private interface SingleCellCall<T> extends BiConsumer<JobManagementServiceStub, StreamObserver<T>> {
        // generics sanity
    }

    private static <T> ErrorMerger<JobManagementServiceStub, T> pointQueriesErrorMerger() {
        return ErrorMerger.grpc(FixedStatusOrder.common());
    }

}

