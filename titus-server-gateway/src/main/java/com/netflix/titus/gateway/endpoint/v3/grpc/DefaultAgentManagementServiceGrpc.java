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
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.DeleteAgentInstanceAttributesRequest;
import com.netflix.titus.grpc.protogen.DeleteInstanceGroupAttributesRequest;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import io.grpc.stub.StreamObserver;

/**
 * @deprecated Remove this stub after confirming that no clients depend on this API.
 */
@Singleton
public class DefaultAgentManagementServiceGrpc extends AgentManagementServiceImplBase {

    @Inject
    public DefaultAgentManagementServiceGrpc() {
    }

    @Override
    public void getInstanceGroups(Empty request, StreamObserver<AgentInstanceGroups> responseObserver) {
        responseObserver.onNext(AgentInstanceGroups.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getInstanceGroup(Id request, StreamObserver<AgentInstanceGroup> responseObserver) {
        responseObserver.onError(AgentManagementException.agentGroupNotFound(request.getId()));
    }

    @Override
    public void getAgentInstance(Id request, StreamObserver<AgentInstance> responseObserver) {
        responseObserver.onError(AgentManagementException.agentNotFound(request.getId()));
    }

    @Override
    public void findAgentInstances(AgentQuery request, StreamObserver<AgentInstances> responseObserver) {
        responseObserver.onNext(AgentInstances.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateInstanceGroupTier(TierUpdate request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentGroupNotFound(request.getInstanceGroupId()));
    }

    @Override
    public void updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentGroupNotFound(request.getInstanceGroupId()));
    }

    @Override
    public void updateInstanceGroupAttributes(InstanceGroupAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentGroupNotFound(request.getInstanceGroupId()));
    }

    @Override
    public void deleteInstanceGroupAttributes(DeleteInstanceGroupAttributesRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentGroupNotFound(request.getInstanceGroupId()));
    }

    @Override
    public void updateAgentInstanceAttributes(AgentInstanceAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentNotFound(request.getAgentInstanceId()));
    }

    @Override
    public void deleteAgentInstanceAttributes(DeleteAgentInstanceAttributesRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(AgentManagementException.agentNotFound(request.getAgentInstanceId()));
    }

    @Override
    public void observeAgents(Empty request, StreamObserver<AgentChangeEvent> responseObserver) {
        // Emit snapshot and never complete
        responseObserver.onNext(AgentChangeEvent.newBuilder()
                .setSnapshotEnd(AgentChangeEvent.SnapshotEnd.getDefaultInstance())
                .build()
        );
    }
}