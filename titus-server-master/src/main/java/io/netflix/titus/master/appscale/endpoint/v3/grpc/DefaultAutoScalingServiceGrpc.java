/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netflix.titus.api.appscale.service.AppScaleManager;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

@Singleton
public class DefaultAutoScalingServiceGrpc extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    private static Logger log = LoggerFactory.getLogger(DefaultAutoScalingServiceGrpc.class);
    private AppScaleManager appScaleManager;


    @Inject
    public DefaultAutoScalingServiceGrpc(AppScaleManager appScaleManager) {
        this.appScaleManager = appScaleManager;
    }

    @Override
    public void setAutoScalingPolicy(com.netflix.titus.grpc.protogen.PutPolicyRequest request,
                                     io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.ScalingPolicyID> responseObserver) {
        appScaleManager.createAutoScalingPolicy(InternalModelConverters.toAutoScalingPolicy(request)).subscribe(
                id -> {
                    responseObserver.onNext(GrpcModelConverters.toScalingPolicyId(id));
                }, throwable -> {
                    responseObserver.onError(
                            new StatusRuntimeException(Status.INTERNAL
                                    .withDescription("Set job scaling policies stream terminated with an error")
                                    .withCause(throwable)));
                },
                () -> {
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void getJobScalingPolicies(com.netflix.titus.grpc.protogen.JobId request,
                                      io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        Observable<AutoScalingPolicy> policyObservable = appScaleManager.getScalingPoliciesForJob(request.getId());
        completePolicyList(policyObservable, responseObserver);
    }

    @Override
    public void getScalingPolicy(com.netflix.titus.grpc.protogen.ScalingPolicyID request,
                                 io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        appScaleManager.getScalingPolicy(request.getId()).subscribe(
                autoScalingPolicyInternal -> {
                    ScalingPolicyResult scalingPolicyResult = GrpcModelConverters.toScalingPolicyResult(autoScalingPolicyInternal);
                    responseObserver.onNext(GetPolicyResult.newBuilder().addItems(scalingPolicyResult).build());
                },
                e->responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("Get job scaling policy stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );
    }

    @Override
    public void getAllScalingPolicies(com.google.protobuf.Empty request,
                                      io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        Observable<AutoScalingPolicy> policyObservable = appScaleManager.getAllScalingPolicies();
        completePolicyList(policyObservable, responseObserver);
    }

    @Override
    public void deleteAutoScalingPolicy(com.netflix.titus.grpc.protogen.DeletePolicyRequest request,
                                        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        appScaleManager.removeAutoScalingPolicy(request.getId().getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                throwable -> {
                    responseObserver.onError(
                            new StatusRuntimeException(Status.INTERNAL
                                    .withDescription("Set job scaling policies stream terminated with an error")
                                    .withCause(throwable)));
                });
    }

    /**
     * Maps policy observable to list and completes.
     * @param policyObservable
     * @param responseObserver
     */
    private void completePolicyList(Observable<AutoScalingPolicy> policyObservable,
            io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        List<ScalingPolicyResult> scalingPolicyResultList = new ArrayList<>();
        policyObservable.subscribe(
                autoScalingPolicyInternal -> {
                    ScalingPolicyResult scalingPolicyResult = GrpcModelConverters.toScalingPolicyResult(autoScalingPolicyInternal);
                    scalingPolicyResultList.add(scalingPolicyResult);
                },
                e->responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("Get job scaling policies stream terminated with an error")
                                .withCause(e))
                ),
                () -> {
                    responseObserver.onNext(GetPolicyResult.newBuilder().addAllItems(scalingPolicyResultList).build());
                    responseObserver.onCompleted();
                }
        );
    }
}
