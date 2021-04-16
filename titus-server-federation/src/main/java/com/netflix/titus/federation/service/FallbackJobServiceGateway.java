package com.netflix.titus.federation.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.*;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

public class FallbackJobServiceGateway {
    private final JobManagementServiceGrpc.JobManagementServiceStub primaryClient;
    private final GrpcConfiguration grpcConfiguration;

    public FallbackJobServiceGateway(
            JobManagementServiceGrpc.JobManagementServiceStub primaryClient,
            GrpcConfiguration grpcConfiguration) {
        this.primaryClient = primaryClient;
        this.grpcConfiguration = grpcConfiguration;
    }

    public Observable<String> createJob(
            JobDescriptor jobDescriptor,
            JobManagementServiceGrpc.JobManagementServiceStub secondaryClient,
            CallMetadata callMetadata) {

        Observable<String> primaryObservable = getCreateObservable(
                primaryClient,
                jobDescriptor,
                callMetadata,
                grpcConfiguration.getPrimaryFallbackTimeoutMs());
        Observable<String> secondaryObservable = getCreateObservable(
                secondaryClient,
                jobDescriptor,
                callMetadata,
                grpcConfiguration.getRequestTimeoutMs());
        return primaryObservable.onErrorResumeNext(secondaryObservable);
    }

    private Observable<String> getCreateObservable(
            JobManagementServiceGrpc.JobManagementServiceStub client,
            JobDescriptor jobDescriptor,
            CallMetadata callMetadata,
            long timeoutMs) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            wrap(client, callMetadata, timeoutMs).createJob(jobDescriptor, streamObserver);
        }, timeoutMs);
    }

    private JobManagementServiceGrpc.JobManagementServiceStub wrap(
            JobManagementServiceGrpc.JobManagementServiceStub client,
            CallMetadata callMetadata,
            long timeoutMs) {
        return createWrappedStub(client, callMetadata, timeoutMs);
    }
}
