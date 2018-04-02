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
package com.netflix.titus.federation.endpoint.grpc;

import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.stub.StreamObserver;
import com.netflix.titus.federation.service.AutoScalingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultAutoScalingServiceGrpc extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(DefaultAutoScalingServiceGrpc.class);
    private AutoScalingService autoScalingService;

    @Inject
    public DefaultAutoScalingServiceGrpc(AutoScalingService autoScalingService) {
        this.autoScalingService = autoScalingService;
    }

    @Override
    public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
        handleMethod(() -> autoScalingService.getAllScalingPolicies(), responseObserver);
    }

    @Override
    public void setAutoScalingPolicy(PutPolicyRequest request, StreamObserver<ScalingPolicyID> responseObserver) {
        handleMethod(() -> autoScalingService.setAutoScalingPolicy(request), responseObserver);
    }

    @Override
    public void getScalingPolicy(ScalingPolicyID request, StreamObserver<GetPolicyResult> responseObserver) {
        handleMethod(() -> autoScalingService.getScalingPolicy(request), responseObserver);
    }

    @Override
    public void getJobScalingPolicies(JobId request, StreamObserver<GetPolicyResult> responseObserver) {
        handleMethod(() -> autoScalingService.getJobScalingPolicies(request), responseObserver);
    }

    @Override
    public void updateAutoScalingPolicy(UpdatePolicyRequest request, StreamObserver<Empty> responseObserver) {
        handleMethod(() -> autoScalingService.updateAutoScalingPolicy(request)
                .andThen(Observable.just(Empty.getDefaultInstance())), responseObserver);
    }

    @Override
    public void deleteAutoScalingPolicy(DeletePolicyRequest request, StreamObserver<Empty> responseObserver) {
        handleMethod(() -> autoScalingService.deleteAutoScalingPolicy(request)
                .andThen(Observable.just(Empty.getDefaultInstance())), responseObserver);
    }

    private <RespT> void handleMethod(Supplier<Observable<RespT>> fnCall, StreamObserver<RespT> responseObserver) {
         Subscription subscription = fnCall.get().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
