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

package com.netflix.titus.master.agent.endpoint.grpc;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toGrpcAgentInstance;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toGrpcAgentInstanceGroup;

@Singleton
public class DefaultAgentManagementServiceGrpc extends AgentManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentManagementServiceGrpc.class);

    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;
    private final SessionContext sessionContext;

    @Inject
    public DefaultAgentManagementServiceGrpc(AgentManagementService agentManagementService,
                                             AgentStatusMonitor agentStatusMonitor,
                                             SessionContext sessionContext) {
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
        this.sessionContext = sessionContext;
    }

    @Override
    public void getInstanceGroups(Empty request, StreamObserver<AgentInstanceGroups> responseObserver) {
        execute(responseObserver, user -> {
            List<AgentInstanceGroup> all = agentManagementService.getInstanceGroups().stream()
                    .map(GrpcAgentModelConverters::toGrpcAgentInstanceGroup)
                    .collect(Collectors.toList());
            responseObserver.onNext(AgentInstanceGroups.newBuilder().addAllAgentInstanceGroups(all).build());
        });
    }

    @Override
    public void getInstanceGroup(Id request, StreamObserver<AgentInstanceGroup> responseObserver) {
        execute(responseObserver, user -> {
            AgentInstanceGroup grpcEntity = toGrpcAgentInstanceGroup(agentManagementService.getInstanceGroup(request.getId()));
            responseObserver.onNext(grpcEntity);
        });
    }

    @Override
    public void getAgentInstance(Id request, StreamObserver<AgentInstance> responseObserver) {
        execute(responseObserver, user -> {
            com.netflix.titus.api.agent.model.AgentInstance instance = agentManagementService.getAgentInstance(request.getId());
            AgentStatus status = agentStatusMonitor.getStatus(request.getId());
            AgentInstance grpcEntity = toGrpcAgentInstance(instance, status);
            responseObserver.onNext(grpcEntity);
        });
    }

    @Override
    public void findAgentInstances(AgentQuery query, StreamObserver<AgentInstances> responseObserver) {
        execute(responseObserver, user -> {
            List<AgentInstance> all = AgentQueryExecutor.findAgentInstances(query, agentManagementService).stream()
                    .map(coreAgentInstance -> {
                        AgentStatus status = agentStatusMonitor.getStatus(coreAgentInstance.getId());
                        return GrpcAgentModelConverters.toGrpcAgentInstance(coreAgentInstance, status);
                    })
                    .collect(Collectors.toList());
            responseObserver.onNext(AgentInstances.newBuilder().addAllAgentInstances(all).build());
        });
    }

    @Override
    public void updateInstanceGroupTier(TierUpdate request, StreamObserver<Empty> responseObserver) {
        agentManagementService.updateInstanceGroupTier(
                request.getInstanceGroupId(),
                GrpcAgentModelConverters.toCoreTier(request.getTier())
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void updateAutoScalingRule(AutoScalingRuleUpdate request, StreamObserver<Empty> responseObserver) {
        agentManagementService.updateAutoScalingRule(
                request.getInstanceGroupId(),
                GrpcAgentModelConverters.toCoreAutoScaleRule(request.getAutoScaleRule())
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate request, StreamObserver<Empty> responseObserver) {
        InstanceGroupLifecycleStatus coreInstanceGroupLifecycleStatus = InstanceGroupLifecycleStatus.newBuilder()
                .withState(GrpcAgentModelConverters.toCoreLifecycleState(request.getLifecycleState()))
                .withDetail(request.getDetail())
                .withTimestamp(System.currentTimeMillis())
                .build();
        agentManagementService.updateInstanceGroupLifecycle(request.getInstanceGroupId(), coreInstanceGroupLifecycleStatus).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void updateInstanceOverrideState(InstanceOverrideStateUpdate request, StreamObserver<Empty> responseObserver) {
        InstanceOverrideStatus coreInstanceOverrideStatus = InstanceOverrideStatus.newBuilder()
                .withState(GrpcAgentModelConverters.toCoreOverrideState(request.getOverrideState()))
                .withDetail(request.getDetail())
                .withTimestamp(System.currentTimeMillis())
                .build();
        agentManagementService.updateInstanceOverride(request.getAgentInstanceId(), coreInstanceOverrideStatus).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void observeAgents(Empty request, StreamObserver<AgentChangeEvent> responseObserver) {
        Observable<AgentEvent> statusUpdateEvents = agentStatusMonitor.monitor().flatMap(update -> {
            if (update.getStatusCode() != AgentStatus.AgentStatusCode.Terminated) {
                return Observable.just(new AgentInstanceUpdateEvent(update.getAgentInstance()));
            }
            return Observable.empty();
        });
        Observable.merge(agentManagementService.events(true), statusUpdateEvents)
                .map(agentEvent -> GrpcAgentModelConverters.toGrpcEvent(agentEvent, agentStatusMonitor)).subscribe(
                event -> {
                    try {
                        event.ifPresent(responseObserver::onNext);
                    } catch (StatusRuntimeException e) {
                        logger.debug("Error during sending event {} to the GRPC client", event, e);
                    }
                },
                responseObserver::onError,
                responseObserver::onCompleted
        );
    }

    private void execute(StreamObserver<?> responseObserver, Consumer<String> action) {
        if (!sessionContext.getCallerId().isPresent()) {
            responseObserver.onError(TitusServiceException.noCallerId());
            return;
        }
        try {
            action.accept(sessionContext.getCallerId().get());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
