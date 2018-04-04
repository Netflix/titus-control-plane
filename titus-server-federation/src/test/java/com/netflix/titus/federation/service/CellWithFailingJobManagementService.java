package com.netflix.titus.federation.service;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import static io.grpc.Status.INTERNAL;

class CellWithFailingJobManagementService extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private final Status errorStatus;

    CellWithFailingJobManagementService() {
        this(INTERNAL);
    }

    CellWithFailingJobManagementService(Status errorStatus) {
        this.errorStatus = errorStatus;
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void findTasks(TaskQuery request, StreamObserver<TaskQueryResult> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }
}

