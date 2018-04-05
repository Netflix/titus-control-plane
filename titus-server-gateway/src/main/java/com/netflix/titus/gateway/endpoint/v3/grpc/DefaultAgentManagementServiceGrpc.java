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
import com.netflix.titus.gateway.service.v3.AgentManagementService;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultAgentManagementServiceGrpc extends AgentManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentManagementServiceGrpc.class);

    private final AgentManagementService agentManagementService;

    @Inject
    public DefaultAgentManagementServiceGrpc(AgentManagementService agentManagementService) {
        this.agentManagementService = agentManagementService;
    }

    @Override
    public void getInstanceGroups(Empty request, StreamObserver<AgentInstanceGroups> responseObserver) {
        Subscription subscription = agentManagementService.getInstanceGroups().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getInstanceGroup(Id request, StreamObserver<AgentInstanceGroup> responseObserver) {
        Subscription subscription = agentManagementService.getInstanceGroup(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getAgentInstance(Id request, StreamObserver<AgentInstance> responseObserver) {
        Subscription subscription = agentManagementService.getAgentInstance(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findAgentInstances(AgentQuery request, StreamObserver<AgentInstances> responseObserver) {
        Subscription subscription = agentManagementService.findAgentInstances(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupTier(TierUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = agentManagementService.updateInstanceGroupTier(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceOverrideState(InstanceOverrideStateUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = agentManagementService.updateInstanceOverride(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateAutoScalingRule(AutoScalingRuleUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = agentManagementService.updateAutoScalingRule(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = agentManagementService.updateInstanceGroupLifecycle(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateInstanceGroupAttributes(InstanceGroupAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = agentManagementService.updateInstanceGroupAttributes(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void observeAgents(Empty request, StreamObserver<AgentChangeEvent> responseObserver) {
        Subscription subscription = agentManagementService.observeAgents().subscribe(
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