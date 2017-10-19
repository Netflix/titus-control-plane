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

package io.netflix.titus.gateway.service.v3.internal;


import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.gateway.service.v3.AutoScalingService;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.functions.Action1;

@Singleton
public class DefaultAutoScalingService implements AutoScalingService {

    private static Logger logger = LoggerFactory.getLogger(DefaultAutoScalingService.class);

    private final GrpcClientConfiguration configuration;
    private AutoScalingServiceGrpc.AutoScalingServiceStub client;

    @Inject
    public DefaultAutoScalingService(GrpcClientConfiguration configuration,
                                     AutoScalingServiceGrpc.AutoScalingServiceStub client) {
        this.configuration = configuration;
        this.client = client;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId request) {
        logger.info("Getting policy for JobId {}", request);
        return toObservable(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.getJobScalingPolicies(request, simpleStreamObserver);
        });
    }

    @Override
    public Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request) {
        logger.info("Setting policy request {}", request);
        return toObservable(emitter -> {
            StreamObserver<ScalingPolicyID> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.setAutoScalingPolicy(request, simpleStreamObserver);
        });
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request) {
        return toObservable(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.getScalingPolicy(request, simpleStreamObserver);
        });
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies() {
        return toObservable(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.getAllScalingPolicies(Empty.newBuilder().build(), simpleStreamObserver);
        });
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.deleteAutoScalingPolicy(request, simpleStreamObserver);
        });
    }

    private Completable toCompletable(Action1<Emitter<Empty>> emitter) {
        return toObservable(emitter).toCompletable();
    }

    private <T> Observable<T> toObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        ).timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }
}
