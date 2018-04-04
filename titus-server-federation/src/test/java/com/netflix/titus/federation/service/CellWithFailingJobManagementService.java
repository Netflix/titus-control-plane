package com.netflix.titus.federation.service;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class CellWithFailingJobManagementService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    @Override
    public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
        responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
}

