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

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.gateway.service.v3.AgentManagementService;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder.AGENT_SANITIZER;
import static io.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestCompletable;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultAgentManagementService implements AgentManagementService {

    private final GrpcClientConfiguration configuration;
    private final AgentManagementServiceStub client;
    private final SessionContext sessionContext;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public DefaultAgentManagementService(GrpcClientConfiguration configuration,
                                         AgentManagementServiceStub client,
                                         SessionContext sessionContext,
                                         @Named(AGENT_SANITIZER) EntitySanitizer entitySanitizer) {
        this.configuration = configuration;
        this.client = client;
        this.sessionContext = sessionContext;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<AgentInstanceGroups> getInstanceGroups() {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<AgentInstanceGroups> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getInstanceGroups(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstanceGroup> getInstanceGroup(String id) {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<AgentInstanceGroup> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getInstanceGroup(Id.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstance> getAgentInstance(String id) {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<AgentInstance> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getAgentInstance(Id.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentInstances> findAgentInstances(AgentQuery query) {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<AgentInstances> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).findAgentInstances(query, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceGroupTier(TierUpdate tierUpdate) {
        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).updateInstanceGroupTier(tierUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateAutoScalingRule(AutoScalingRuleUpdate autoScalingRuleUpdate) {
        AutoScaleRule coreAutoScaleRule = GrpcAgentModelConverters.toCoreAutoScaleRule(autoScalingRuleUpdate.getAutoScaleRule());
        Set<ConstraintViolation<AutoScaleRule>> violations = entitySanitizer.validate(coreAutoScaleRule);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).updateAutoScalingRule(autoScalingRuleUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).updateInstanceGroupLifecycleState(lifecycleStateUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateInstanceOverride(InstanceOverrideStateUpdate overrideStateUpdate) {
        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).updateInstanceOverrideState(overrideStateUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<AgentChangeEvent> observeAgents() {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<AgentChangeEvent> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).observeAgents(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }
}
