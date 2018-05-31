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

import com.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.HealthService;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultHealthService implements HealthService {
    private final GrpcClientConfiguration configuration;
    private final CallMetadataResolver callMetadataResolver;
    private final HealthGrpc.HealthStub client;

    @Inject
    public DefaultHealthService(GrpcClientConfiguration configuration, CallMetadataResolver callMetadataResolver, HealthGrpc.HealthStub client) {
        this.configuration = configuration;
        this.callMetadataResolver = callMetadataResolver;
        this.client = client;
    }

    @Override
    public Observable<HealthCheckResponse> check(HealthCheckRequest request) {
        return createRequestObservable(emitter -> {
            StreamObserver<HealthCheckResponse> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).check(request, streamObserver);
        }, configuration.getRequestTimeout());
    }
}
