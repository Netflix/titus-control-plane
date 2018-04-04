package com.netflix.titus.federation.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import io.grpc.stub.StreamObserver;

import static io.grpc.Status.NOT_FOUND;

class CellWithJobIds extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final List<String> jobIds;

    private final Set<String> capacityUpdatesTracking = new HashSet<>();
    private final Set<String> statusUpdatesTracking = new HashSet<>();
    private final Set<String> processUpdatesTracking = new HashSet<>();

    CellWithJobIds(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        if (!jobIds.contains(request.getId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        responseObserver.onNext(Job.newBuilder().setId(request.getId()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        if (!jobIds.contains(request.getJobId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        capacityUpdatesTracking.add(request.getJobId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        if (!jobIds.contains(request.getId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        statusUpdatesTracking.add(request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        if (!jobIds.contains(request.getJobId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        processUpdatesTracking.add(request.getJobId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public boolean containsCapacityUpdates(String jobId) {
        return capacityUpdatesTracking.contains(jobId);
    }

    public boolean containsStatusUpdates(String jobId) {
        return statusUpdatesTracking.contains(jobId);
    }

    public boolean containsProcessUpdates(String jobId) {
        return processUpdatesTracking.contains(jobId);
    }
}

