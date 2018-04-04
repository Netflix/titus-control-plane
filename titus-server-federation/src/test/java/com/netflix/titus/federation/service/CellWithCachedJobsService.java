package com.netflix.titus.federation.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.common.grpc.GrpcUtil;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.grpc.stub.StreamObserver;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;

class CellWithCachedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final String cellName;
    private final Map<JobId, JobDescriptor> jobDescriptorMap = new HashMap<>();

    CellWithCachedJobsService(String name) {
        this.cellName = name;
    }

    @Override
    public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
        JobId jobId = JobId.newBuilder().setId(UUID.randomUUID().toString()).build();
        jobDescriptorMap.put(
                jobId,
                JobDescriptor.newBuilder(request)
                        .putAttributes(JOB_ATTRIBUTES_CELL, cellName)
                        .build());
        Subscription subscription = Observable.just(jobId)
                .subscribe(
                        responseObserver::onNext,
                        responseObserver::onError,
                        responseObserver::onCompleted
                );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        final Subscription subscription = Observable.just(Job.newBuilder().setJobDescriptor(jobDescriptorMap.get(request)).build())
                .subscribe(
                        responseObserver::onNext,
                        responseObserver::onError,
                        responseObserver::onCompleted
                );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }
}

