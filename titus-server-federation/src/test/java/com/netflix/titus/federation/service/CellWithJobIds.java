package com.netflix.titus.federation.service;

import java.util.List;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class CellWithJobIds extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private List<String> jobIds;

    CellWithJobIds(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        if (jobIds.contains(request.getId())) {
            responseObserver.onNext(Job.newBuilder().setId(request.getId()).build());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
    }
}

