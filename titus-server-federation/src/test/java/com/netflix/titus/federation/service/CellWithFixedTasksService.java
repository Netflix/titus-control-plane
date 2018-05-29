package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.common.util.ProtobufCopy;
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
    private final Map<String, Task> tasksIndex;

    CellWithFixedTasksService(List<Task> snapshot) {
        this.tasksIndex = snapshot.stream().collect(Collectors.toMap(Task::getId, Function.identity()));
    }

    private List<Task> getTasksList() {
        return new ArrayList<>(tasksIndex.values());
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
                getTasksList(),
                JobManagerCursors.taskCursorOrderComparator(),
                JobManagerCursors::taskIndexOf,
                JobManagerCursors::newCursorFrom
        );
        Set<String> fieldsFilter = new HashSet<>(request.getFieldsList());
        if (!fieldsFilter.isEmpty()) {
            fieldsFilter.add("id");
            page = page.mapLeft(tasks -> tasks.stream()
                    .map(task -> ProtobufCopy.copy(task, fieldsFilter))
                    .collect(Collectors.toList()));
        }
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
        tasksIndex.remove(request.getTaskId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    Map<String, Task> currentTasks() {
        return Collections.unmodifiableMap(tasksIndex);
    }
}

