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

package com.netflix.titus.runtime.connector.agent.client;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class GrpcAgentManagementClient implements AgentManagementClient {

    private final GrpcClientConfiguration configuration;
    private final AgentManagementServiceStub client;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public GrpcAgentManagementClient(GrpcClientConfiguration configuration,
                                     AgentManagementServiceStub client,
                                     CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Observable<AgentInstanceGroups> getInstanceGroups() {
        return createRequestObservable(emitter -> {
            StreamObserver<AgentInstanceGroups> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getInstanceGroups(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstanceGroup> getInstanceGroup(String id) {
        return createRequestObservable(emitter -> {
            StreamObserver<AgentInstanceGroup> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getInstanceGroup(Id.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstance> getAgentInstance(String id) {
        return createRequestObservable(emitter -> {
            StreamObserver<AgentInstance> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getAgentInstance(Id.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstances> findAgentInstances(AgentQuery query) {
        return createRequestObservable(emitter -> {
            StreamObserver<AgentInstances> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findAgentInstances(query, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceGroupTier(TierUpdate tierUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateInstanceGroupTier(tierUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateInstanceGroupLifecycleState(lifecycleStateUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceGroupAttributes(InstanceGroupAttributesUpdate attributesUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateInstanceGroupAttributes(attributesUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateAgentInstanceAttributes(AgentInstanceAttributesUpdate attributesUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateAgentInstanceAttributes(attributesUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentChangeEvent> observeAgents() {
        return createRequestObservable(emitter -> {
            StreamObserver<AgentChangeEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeAgents(Empty.getDefaultInstance(), streamObserver);
        });
    }
}
