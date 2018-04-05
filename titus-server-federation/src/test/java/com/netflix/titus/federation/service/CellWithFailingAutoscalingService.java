package com.netflix.titus.federation.service;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class CellWithFailingAutoscalingService extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    @Override
    public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
        responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
    }
}

