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

import com.netflix.titus.gateway.service.v3.SchedulerService;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceStub;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;

@Singleton
public class DefaultSchedulerService implements SchedulerService {
    private final GrpcClientConfiguration configuration;
    private final SchedulerServiceStub client;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultSchedulerService(GrpcClientConfiguration configuration,
                                   SchedulerServiceStub client,
                                   CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Observable<SchedulingResultEvent> findLastSchedulingResult(String taskId) {
        return createRequestObservable(emitter -> {
            StreamObserver<SchedulingResultEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            GrpcUtil.createWrappedStubWithResolver(client, callMetadataResolver, configuration.getRequestTimeout()).getSchedulingResult(SchedulingResultRequest.newBuilder().setTaskId(taskId).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SchedulingResultEvent> observeSchedulingResults(String taskId) {
        return createRequestObservable(emitter -> {
            StreamObserver<SchedulingResultEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            GrpcUtil.createWrappedStubWithResolver(client, callMetadataResolver).observeSchedulingResults(SchedulingResultRequest.newBuilder().setTaskId(taskId).build(), streamObserver);
        });
    }
}
