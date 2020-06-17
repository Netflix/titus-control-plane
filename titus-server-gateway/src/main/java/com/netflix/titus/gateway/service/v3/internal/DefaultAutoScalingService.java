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

package com.netflix.titus.gateway.service.v3.internal;


import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceStub;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.service.AutoScalingService;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultAutoScalingService implements AutoScalingService {

    private static Logger logger = LoggerFactory.getLogger(DefaultAutoScalingService.class);

    private final GrpcClientConfiguration configuration;
    private AutoScalingServiceStub client;

    @Inject
    public DefaultAutoScalingService(GrpcClientConfiguration configuration, AutoScalingServiceStub client) {
        this.configuration = configuration;
        this.client = client;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId request, CallMetadata callMetadata) {
        logger.info("Getting policy for JobId {}", request);
        return createRequestObservable(emitter -> {
            StreamObserver<GetPolicyResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).getJobScalingPolicies(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request, CallMetadata callMetadata) {
        logger.info("Setting policy request {}", request);
        return createRequestObservable(emitter -> {
            StreamObserver<ScalingPolicyID> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).setAutoScalingPolicy(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetPolicyResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).getScalingPolicy(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies(CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetPolicyResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).getAllScalingPolicies(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).deleteAutoScalingPolicy(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateAutoScalingPolicy(UpdatePolicyRequest request, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).updateAutoScalingPolicy(request, streamObserver);
        }, configuration.getRequestTimeout());
    }
}
