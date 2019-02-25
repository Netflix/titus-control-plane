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

package com.netflix.titus.gateway.endpoint.v3.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.connector.agent.ReactorAgentManagementServiceStub;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultAgentManagementServiceGrpc extends AgentManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentManagementServiceGrpc.class);

    private final ReactorAgentManagementServiceStub agentManagementService;

    @Inject
    public DefaultAgentManagementServiceGrpc(ReactorAgentManagementServiceStub agentManagementService) {
        this.agentManagementService = agentManagementService;
    }

    @Override
    public void getInstanceGroups(Empty request, StreamObserver<AgentInstanceGroups> responseObserver) {
        Disposable subscription = agentManagementService.getInstanceGroups().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getInstanceGroup(Id request, StreamObserver<AgentInstanceGroup> responseObserver) {
        Disposable subscription = agentManagementService.getInstanceGroup(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getAgentInstance(Id request, StreamObserver<AgentInstance> responseObserver) {
        Disposable subscription = agentManagementService.getAgentInstance(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findAgentInstances(AgentQuery request, StreamObserver<AgentInstances> responseObserver) {
        Disposable subscription = agentManagementService.findAgentInstances(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupTier(TierUpdate request, StreamObserver<Empty> responseObserver) {
        Disposable subscription = agentManagementService.updateInstanceGroupTier(request).subscribe(
                next -> {
                    // Never
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> emitEmptyReply(responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate request, StreamObserver<Empty> responseObserver) {
        Disposable subscription = agentManagementService.updateInstanceGroupLifecycleState(request).subscribe(
                next -> {
                    // Never
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> emitEmptyReply(responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupAttributes(InstanceGroupAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        Disposable subscription = agentManagementService.updateInstanceGroupAttributes(request).subscribe(
                next -> {
                    // Never
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> emitEmptyReply(responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateAgentInstanceAttributes(AgentInstanceAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        Disposable subscription = agentManagementService.updateAgentInstanceAttributes(request).subscribe(
                next -> {
                    // Never
                },
                e -> safeOnError(logger, e, responseObserver),
                () -> emitEmptyReply(responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void observeAgents(Empty request, StreamObserver<AgentChangeEvent> responseObserver) {
        Disposable subscription = agentManagementService.observeAgents().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    private void emitEmptyReply(StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}