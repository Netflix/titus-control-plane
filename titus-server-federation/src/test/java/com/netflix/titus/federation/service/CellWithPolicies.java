package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.stub.StreamObserver;

class CellWithPolicies extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    private final Map<String, ScalingPolicyResult> policyMap;

    CellWithPolicies(List<ScalingPolicyResult> policyResults) {
        policyMap = policyResults.stream().collect(Collectors.toMap(p -> p.getId().getId(), p -> p));
    }

    @Override
    public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
        sendScalingPolicyResults(new ArrayList<>(policyMap.values()), responseObserver);
    }

    @Override
    public void getJobScalingPolicies(JobId request, StreamObserver<GetPolicyResult> responseObserver) {
        List<ScalingPolicyResult> scalingPolicyResults = policyMap.values().stream()
                .filter(p -> p.getJobId().equals(request.getId()))
                .collect(Collectors.toList());
        sendScalingPolicyResults(scalingPolicyResults, responseObserver);
    }

    @Override
    public void getScalingPolicy(ScalingPolicyID request, StreamObserver<GetPolicyResult> responseObserver) {
        List<ScalingPolicyResult> result = policyMap.values().stream()
                .filter(p -> p.getId().getId().equals(request.getId())).collect(Collectors.toList());
        sendScalingPolicyResults(result, responseObserver);
    }

    @Override
    public void setAutoScalingPolicy(PutPolicyRequest request, StreamObserver<ScalingPolicyID> responseObserver) {
        ScalingPolicyID newPolicyId = ScalingPolicyID.newBuilder().setId(UUID.randomUUID().toString()).build();
        ScalingPolicyResult newPolicyResult = ScalingPolicyResult.newBuilder().setId(newPolicyId).setJobId(request.getJobId()).build();

        policyMap.put(newPolicyId.getId(), newPolicyResult);
        responseObserver.onNext(newPolicyId);
        responseObserver.onCompleted();
    }

    @Override
    public void updateAutoScalingPolicy(UpdatePolicyRequest request, StreamObserver<Empty> responseObserver) {
        if (policyMap.containsKey(request.getPolicyId().getId())) {
            ScalingPolicyResult currentPolicy = policyMap.get(request.getPolicyId().getId());
            ScalingPolicyResult updatePolicy = ScalingPolicyResult.newBuilder().setId(currentPolicy.getId()).setJobId(currentPolicy.getJobId())
                    .setScalingPolicy(request.getScalingPolicy()).build();
            policyMap.put(currentPolicy.getId().getId(), updatePolicy);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(new IllegalArgumentException(request.getPolicyId().getId() + " is Invalid"));
        }
    }

    @Override
    public void deleteAutoScalingPolicy(DeletePolicyRequest request, StreamObserver<Empty> responseObserver) {
        if (policyMap.containsKey(request.getId().getId())) {
            policyMap.remove(request.getId().getId());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(new IllegalArgumentException(request.getId().getId() + " is Invalid"));
        }
    }

    private void sendScalingPolicyResults(List<ScalingPolicyResult> results,
                                          StreamObserver<GetPolicyResult> responseObserver) {
        GetPolicyResult.Builder getPolicyResultBuilder = GetPolicyResult.newBuilder();
        results.forEach(getPolicyResultBuilder::addItems);
        responseObserver.onNext(getPolicyResultBuilder.build());
        responseObserver.onCompleted();
    }
}
