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


import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.gateway.service.v3.AutoScalingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DefaultAutoScalingService implements AutoScalingService {
    private static Logger log = LoggerFactory.getLogger(DefaultAutoScalingService.class);
    private AutoScalingServiceGrpc.AutoScalingServiceStub client;


    @Inject
    public DefaultAutoScalingService(AutoScalingServiceGrpc.AutoScalingServiceStub client) {
        this.client = client;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId request) {
        log.info("Getting policy for JobId {}", request);
        return Observable.create(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = StreamObserverHelper.createSimpleStreamObserver(emitter);
            client.getJobScalingPolicies(request, simpleStreamObserver);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public  Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request) {
        log.info("Setting policy request {}", request);
        return Observable.create(emitter -> {
            StreamObserver<ScalingPolicyID> simpleStreamObserver = StreamObserverHelper.createSimpleStreamObserver(emitter);
            client.setAutoScalingPolicy(request, simpleStreamObserver);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request) {
        return Observable.create(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = StreamObserverHelper.createSimpleStreamObserver(emitter);
            client.getScalingPolicy(request, simpleStreamObserver);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies() {
        return Observable.create(emitter -> {
            StreamObserver<GetPolicyResult> simpleStreamObserver = StreamObserverHelper.createSimpleStreamObserver(emitter);
            client.getAllScalingPolicies(Empty.newBuilder().build(), simpleStreamObserver);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request) {
        return Observable.<Empty>create(emitter -> {
            StreamObserver<Empty> simpleStreamObserver = StreamObserverHelper.createSimpleStreamObserver(emitter);
            client.deleteAutoScalingPolicy(request, simpleStreamObserver);
        }, Emitter.BackpressureMode.NONE).toCompletable();
    }
}
