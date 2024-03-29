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

package com.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.sanitizer.ScalingPolicySanitizerBuilder;
import com.netflix.titus.api.appscale.service.AppScaleManager;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.AlarmConfiguration;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultAutoScalingServiceGrpc extends AutoScalingServiceGrpc.AutoScalingServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAutoScalingServiceGrpc.class);
    private AppScaleManager appScaleManager;
    private EntitySanitizer entitySanitizer;

    @Inject
    public DefaultAutoScalingServiceGrpc(AppScaleManager appScaleManager, @Named(ScalingPolicySanitizerBuilder.SCALING_POLICY_SANITIZER) EntitySanitizer entitySanitizer) {
        this.appScaleManager = appScaleManager;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public void setAutoScalingPolicy(com.netflix.titus.grpc.protogen.PutPolicyRequest request,
                                     io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.ScalingPolicyID> responseObserver) {
        validateAndConvertAutoScalingPolicyToCoreModel(request).flatMap(
                validatedRequest -> ReactorExt.toMono(appScaleManager.createAutoScalingPolicy(validatedRequest).toSingle()))
                .subscribe(
                        id -> responseObserver.onNext(GrpcModelConverters.toScalingPolicyId(id)),
                        e -> safeOnError(logger, e, responseObserver),
                        () -> responseObserver.onCompleted());
    }

    private Mono<AutoScalingPolicy> validateAndConvertAutoScalingPolicyToCoreModel(PutPolicyRequest request) {
        return Mono.defer(() -> {
            AutoScalingPolicy autoScalingPolicy;
            try {
                autoScalingPolicy = InternalModelConverters.toAutoScalingPolicy(request);
            } catch (Exception exe) {
                return Mono.error(TitusServiceException.invalidArgument("Error when converting GRPC object to internal representation: " + exe.getMessage()));
            }
            return Mono.fromCallable(() -> entitySanitizer.validate(autoScalingPolicy))
                    .flatMap(violations -> {
                        if (!violations.isEmpty()) {
                            return Mono.error(TitusServiceException.invalidArgument(violations));
                        }
                        return Mono.just(autoScalingPolicy);
                    });
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
        // FIXME: make NOT_FOUND an explicit error condition
        // appScaleManager.getScalingPolicy(id) will return an empty Observable when the id is not found, which makes
        // the gRPC handler throw an exception (INTERNAL: Completed without a response). This error should be an
        // explicit condition of the API, and mapped to Status.NOT_FOUND
        appScaleManager.getScalingPolicy(request.getId()).subscribe(
                autoScalingPolicyInternal -> {
                    ScalingPolicyResult scalingPolicyResult = GrpcModelConverters.toScalingPolicyResult(autoScalingPolicyInternal);
                    responseObserver.onNext(GetPolicyResult.newBuilder().addItems(scalingPolicyResult).build());
                },
                e -> safeOnError(logger, e, responseObserver),
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
                e -> safeOnError(logger, e, responseObserver));
    }

    @Override
    public void updateAutoScalingPolicy(UpdatePolicyRequest request, io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        appScaleManager.updateAutoScalingPolicy(InternalModelConverters.toAutoScalingPolicy(request))
                .subscribe(
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        },
                        e -> safeOnError(logger, e, responseObserver)
                );
    }

    private void completePolicyList(Observable<AutoScalingPolicy> policyObservable,
                                    io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.GetPolicyResult> responseObserver) {
        List<ScalingPolicyResult> scalingPolicyResultList = new ArrayList<>();
        policyObservable.subscribe(
                autoScalingPolicyInternal -> {
                    ScalingPolicyResult scalingPolicyResult = GrpcModelConverters.toScalingPolicyResult(autoScalingPolicyInternal);
                    scalingPolicyResultList.add(scalingPolicyResult);
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> {
                    responseObserver.onNext(GetPolicyResult.newBuilder().addAllItems(scalingPolicyResultList).build());
                    responseObserver.onCompleted();
                }
        );
    }
}
