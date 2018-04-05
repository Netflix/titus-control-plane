package com.netflix.titus.federation.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.stub.StreamObserver;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static io.grpc.Status.NOT_FOUND;

class CellWithFixedTasksService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private List<Task> snapshot;
    private final Map<String, Task> tasksIndex;

    CellWithFixedTasksService(List<Task> snapshot) {
        this.snapshot = snapshot;
        this.tasksIndex = snapshot.stream().collect(Collectors.toMap(Task::getId, Function.identity()));
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        Task task = tasksIndex.get(request.getId());
        if (task == null) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        responseObserver.onNext(task);
        responseObserver.onCompleted();
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

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        if (!tasksIndex.containsKey(request.getTaskId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        // none of this is thread safe, but it is probably OK since it is used only in tests
        tasksIndex.remove(request.getTaskId());
        snapshot = snapshot.stream()
                .filter(task -> !task.getId().equals(request.getTaskId()))
                .collect(Collectors.toList());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    Map<String, Task> currentTasks() {
        return Collections.unmodifiableMap(tasksIndex);
    }
}

