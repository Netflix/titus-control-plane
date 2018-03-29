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

package io.netflix.titus.federation.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.TaskUpdate;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.EmitterWithMultipleSubscriptions;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.concurrency.CallbackCountDownLatch;
import io.netflix.titus.federation.startup.GrpcConfiguration;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters;
import io.netflix.titus.runtime.jobmanager.JobManagerCursors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_STACK;
import static io.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_STACK;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;

@Singleton
public class AggregatingJobManagementService implements JobManagementService {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingJobManagementService.class);
    private final GrpcConfiguration grpcConfiguration;
    private final TitusFederationConfiguration federationConfiguration;
    private final CellConnector connector;
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
        JobManagementServiceStub client = optionalClient.get();

        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            createWrappedStub(client, sessionContext, grpcConfiguration.getRequestTimeoutMs()).createJob(jobDescriptor, streamObserver);
        }, grpcConfiguration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        return Completable.error(notImplemented("updateJobCapacity"));
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return Completable.error(notImplemented("updateJobProcesses"));
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate) {
        return Completable.error(notImplemented("updateJobStatus"));
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        return Observable.error(notImplemented("findJob"));
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery request) {
        if (StringExt.isNotEmpty(request.getPage().getCursor()) || request.getPage().getPageNumber() == 0) {
            return findJobsWithCursorPagination(request);
        }
        // TODO: page number pagination
        return Observable.error(notImplemented("findJobsWithLegacyPagination"));
    }

    private Observable<JobQueryResult> findJobsWithCursorPagination(JobQuery request) {
        Map<Cell, JobManagementServiceStub> clients = CellConnectorUtil.stubs(connector, JobManagementServiceGrpc::newStub);
        List<Observable<JobQueryResult>> requests = clients.values().stream()
                .map(client -> findJobsInCell(client, request))
                .collect(Collectors.toList());

        return Observable.combineLatest(requests, (results) -> {
            final JobQueryResult.Builder builder = JobQueryResult.newBuilder();

            final List<Job> allJobs = Arrays.stream(results)
                    .map(JobQueryResult.class::cast)
                    .flatMap(result -> result.getItemsList().stream())
                    .sorted(JobManagerCursors.jobCursorOrderComparator())
                    .collect(Collectors.toList());

            Pagination combinedPagination = combinePagination(request, results)
                    .orElse(new Pagination(toPage(request.getPage()), false, 0, 0, ""));

            // TODO: refine the pageNumber estimation with more information from each Cell
            int lastItemOffset = Math.min(allJobs.size(), request.getPage().getPageSize());
            List<Job> pageItems = allJobs.subList(0, lastItemOffset);
            String cursor = allJobs.isEmpty() ? "" : JobManagerCursors.newCursorFrom(pageItems.get(pageItems.size() - 1));
            boolean hasMore = combinedPagination.hasMore() || lastItemOffset < allJobs.size();

            builder.addAllItems(pageItems);
            builder.setPagination(toGrpcPagination(combinedPagination).toBuilder()
                    .setCursor(cursor)
                    .setHasMore(hasMore)
            );
            return builder.build();
        });
    }

    private Optional<Pagination> combinePagination(JobQuery request, Object[] results) {
        return Arrays.stream(results)
                .map(JobQueryResult.class::cast)
                .map(JobQueryResult::getPagination)
                .map(CommonGrpcModelConverters::toPagination)
                .reduce((one, other) -> {
                    // first estimation of pageNumber is based on how many pages exist before the cursor on each Cell
                    int estimatedPageNumber = one.getCurrentPage().getPageNumber() + other.getCurrentPage().getPageNumber();
                    return new Pagination(
                            Page.newBuilder()
                                    .withPageNumber(estimatedPageNumber)
                                    .withPageSize(request.getPage().getPageSize())
                                    .withCursor(request.getPage().getCursor())
                                    .build(),
                            one.hasMore() || other.hasMore(),
                            one.getTotalPages() + other.getTotalPages(),
                            one.getTotalItems() + other.getTotalItems(),
                            "" // compute a combined cursor later
                    );
                });
    }

    private Observable<JobQueryResult> findJobsInCell(JobManagementServiceStub client, JobQuery request) {
        return GrpcUtil.<JobQueryResult>createRequestObservable(emitter -> {
            final StreamObserver<JobQueryResult> streamObserver = GrpcUtil.createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, sessionContext, grpcConfiguration.getRequestTimeoutMs()).findJobs(request, streamObserver);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return Observable.error(notImplemented("observeJob"));
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
                createWrappedStub(client, sessionContext).observeJobs(Empty.getDefaultInstance(), streamObserver);
            });
        });
        return observable.map(this::addStackName);
    }

    @Override
    public Completable killJob(String jobId) {
        return Completable.error(notImplemented("killJob"));
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        return Observable.error(notImplemented("findTask"));
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery) {
        return Observable.error(notImplemented("findTasks"));
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest) {
        return Completable.error(notImplemented("killTask"));
    }

    private JobChangeNotification addStackName(JobChangeNotification notification) {
        switch (notification.getNotificationCase()) {
            case JOBUPDATE:
                JobDescriptor jobDescriptor = notification.getJobUpdate().getJob().getJobDescriptor().toBuilder()
                        .putAttributes(JOB_ATTRIBUTES_STACK, federationConfiguration.getStack())
                        .build();
                Job job = notification.getJobUpdate().getJob().toBuilder().setJobDescriptor(jobDescriptor).build();
                JobChangeNotification.JobUpdate jobUpdate = notification.getJobUpdate().toBuilder().setJob(job).build();
                return notification.toBuilder().setJobUpdate(jobUpdate).build();
            case TASKUPDATE:
                final Task.Builder taskBuilder = notification.getTaskUpdate().getTask().toBuilder()
                        .putTaskContext(TASK_ATTRIBUTES_STACK, federationConfiguration.getStack());
                final TaskUpdate.Builder taskUpdate = notification.getTaskUpdate().toBuilder().setTask(taskBuilder);
                return notification.toBuilder().setTaskUpdate(taskUpdate).build();
            default:
                return notification;
        }
    }

    private static StatusException notImplemented(String operation) {
        return Status.UNIMPLEMENTED.withDescription(operation + " is not implemented").asException();
    }

    private static JobChangeNotification buildJobSnapshotEndMarker() {
        final JobChangeNotification.SnapshotEnd marker = JobChangeNotification.SnapshotEnd.newBuilder().build();
        return JobChangeNotification.newBuilder().setSnapshotEnd(marker).build();
    }
}

