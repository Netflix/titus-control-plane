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

package com.netflix.titus.runtime.connector.supervisor.client;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.grpc.protogen.MasterInstanceId;
import com.netflix.titus.grpc.protogen.MasterInstances;
import com.netflix.titus.grpc.protogen.SupervisorEvent;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.supervisor.SupervisorClient;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.*;

@Singleton
public class GrpcSupervisorClient implements SupervisorClient {

    private final GrpcClientConfiguration configuration;
    private final SupervisorServiceStub client;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public GrpcSupervisorClient(GrpcClientConfiguration configuration,
                                SupervisorServiceStub client,
                                CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Observable<MasterInstances> getMasterInstances() {
        return createRequestObservable(emitter -> {
            StreamObserver<MasterInstances> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getMasterInstances(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<MasterInstance> getMasterInstance(String instanceId) {
        return createRequestObservable(emitter -> {
            StreamObserver<MasterInstance> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout())
                    .getMasterInstance(MasterInstanceId.newBuilder().setInstanceId(instanceId).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SupervisorEvent> observeEvents() {
        return createRequestObservable(emitter -> {
            StreamObserver<SupervisorEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeEvents(Empty.getDefaultInstance(), streamObserver);
        });
    }

    @Override
    public Completable stopBeingLeader() {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).stopBeingLeader(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }
}
