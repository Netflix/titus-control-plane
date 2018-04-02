/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.gateway.endpoint.v3.grpc;


import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.stub.StreamObserver;
import com.netflix.titus.gateway.service.v3.AutoScalingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultAutoScalingServiceGrpc extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    private static Logger log = LoggerFactory.getLogger(DefaultAutoScalingServiceGrpc.class);
    private final AutoScalingService autoScalingService;

    @Inject
    public DefaultAutoScalingServiceGrpc(AutoScalingService autoScalingService) {
        this.autoScalingService = autoScalingService;
    }

    @Override
    public void getAllScalingPolicies(com.google.protobuf.Empty request,
                                      io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        Subscription subscription = autoScalingService.getAllScalingPolicies().subscribe(
                responseObserver::onNext,
                e -> safeOnError(log, e, responseObserver),
                responseObserver::onCompleted
        );

        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getScalingPolicy(com.netflix.titus.grpc.protogen.ScalingPolicyID request,
                                 io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        Subscription subscription = autoScalingService.getScalingPolicy(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(log, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getJobScalingPolicies(JobId request, StreamObserver<GetPolicyResult> responseObserver) {
        log.info("Gateway getAutoScalingPolicy (gRPC) with request {}", request);

        Subscription subscription = autoScalingService.getJobScalingPolicies(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(log, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void setAutoScalingPolicy(com.netflix.titus.grpc.protogen.PutPolicyRequest request,
                                     io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.ScalingPolicyID> responseObserver) {
        Subscription subscription = autoScalingService.setAutoScalingPolicy(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(log, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void deleteAutoScalingPolicy(com.netflix.titus.grpc.protogen.DeletePolicyRequest request,
                                        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        Subscription subscription = autoScalingService.deleteAutoScalingPolicy(request).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(log, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateAutoScalingPolicy(UpdatePolicyRequest request, io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        Subscription subscription = autoScalingService.updateAutoScalingPolicy(request).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(log, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
