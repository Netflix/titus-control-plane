package com.netflix.titus.federation.service;

import java.util.List;

import com.google.protobuf.Empty;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.common.grpc.GrpcUtil;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.SnapshotEnd;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.stub.StreamObserver;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;

class CellWithFixedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final List<Job> snapshot;
    private final Observable<JobChangeNotification> updates;

    CellWithFixedJobsService(List<Job> snapshot, Observable<JobChangeNotification> updates) {
        this.snapshot = snapshot;
        this.updates = updates;
    }

    @Override
    public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
        Pair<List<Job>, Pagination> page = PaginationUtil.takePageWithCursor(
                toPage(request.getPage()),
                snapshot,
                JobManagerCursors.jobCursorOrderComparator(),
                JobManagerCursors::jobIndexOf,
                JobManagerCursors::newCursorFrom
        );
        JobQueryResult result = JobQueryResult.newBuilder()
                .addAllItems(page.getLeft())
                .setPagination(toGrpcPagination(page.getRight()))
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
        for (Job job : snapshot) {
            JobChangeNotification.JobUpdate update = JobChangeNotification.JobUpdate.newBuilder().setJob(job).build();
            JobChangeNotification notification = JobChangeNotification.newBuilder().setJobUpdate(update).build();
            responseObserver.onNext(notification);
        }
        SnapshotEnd snapshotEnd = SnapshotEnd.newBuilder().build();
        JobChangeNotification marker = JobChangeNotification.newBuilder().setSnapshotEnd(snapshotEnd).build();
        responseObserver.onNext(marker);

        final Subscription subscription = updates.subscribe(
                responseObserver::onNext,
                responseObserver::onError,
                responseObserver::onCompleted
        );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

}
