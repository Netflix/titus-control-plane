package com.netflix.titus.federation.service;

import java.util.List;

import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.stub.StreamObserver;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;

class CellWithFixedTasksService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final List<Task> snapshot;

    CellWithFixedTasksService(List<Task> snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public void findTasks(TaskQuery request, StreamObserver<TaskQueryResult> responseObserver) {
        Pair<List<Task>, Pagination> page = PaginationUtil.takePageWithCursor(
                toPage(request.getPage()),
                snapshot,
                JobManagerCursors.taskCursorOrderComparator(),
                JobManagerCursors::taskIndexOf,
                JobManagerCursors::newCursorFrom
        );
        TaskQueryResult result = TaskQueryResult.newBuilder()
                .addAllItems(page.getLeft())
                .setPagination(toGrpcPagination(page.getRight()))
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}

