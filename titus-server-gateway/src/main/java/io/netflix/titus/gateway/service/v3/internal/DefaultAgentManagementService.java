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
import java.util.concurrent.TimeUnit;
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
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceStub;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
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
import rx.Emitter;
import rx.Observable;
import rx.functions.Action1;

import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_FIND_AGENT_INSTANCES;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_GET_AGENT_INSTANCE;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_OBSERVE_AGENTS;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_UPDATE_AUTO_SCALING_RULE;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_UPDATE_INSTANCE_GROUP_LIFECYCLE_STATE;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_UPDATE_INSTANCE_GROUP_TIER;
import static com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.METHOD_UPDATE_INSTANCE_OVERRIDE_STATE;
import static io.netflix.titus.gateway.service.v3.internal.GrpcServiceUtil.getRxJavaAdjustedTimeout;
import static io.netflix.titus.runtime.TitusEntitySanitizerModule.AGENT_SANITIZER;

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
        return toObservable(emitter -> {
            StreamObserver<AgentInstanceGroups> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(AgentManagementServiceGrpc.METHOD_GET_INSTANCE_GROUPS, Empty.getDefaultInstance(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<AgentInstanceGroup> getInstanceGroup(String id) {
        return toObservable(emitter -> {
            StreamObserver<AgentInstanceGroup> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(AgentManagementServiceGrpc.METHOD_GET_INSTANCE_GROUP, Id.newBuilder().setId(id).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<AgentInstance> getAgentInstance(String id) {
        return Observable.create(emitter -> {
            StreamObserver<AgentInstance> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_GET_AGENT_INSTANCE, Id.newBuilder().setId(id).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<AgentInstances> findAgentInstances(AgentQuery query) {
        return toObservable(emitter -> {
            StreamObserver<AgentInstances> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_FIND_AGENT_INSTANCES, query, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable updateInstanceGroupTier(TierUpdate tierUpdate) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_UPDATE_INSTANCE_GROUP_TIER, tierUpdate, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable updateAutoScalingRule(AutoScalingRuleUpdate autoScalingRuleUpdate) {
        AutoScaleRule coreAutoScaleRule = GrpcAgentModelConverters.toCoreAutoScaleRule(autoScalingRuleUpdate.getAutoScaleRule());
        Set<ConstraintViolation<AutoScaleRule>> violations = entitySanitizer.validate(coreAutoScaleRule);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_UPDATE_AUTO_SCALING_RULE, autoScalingRuleUpdate, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_UPDATE_INSTANCE_GROUP_LIFECYCLE_STATE, lifecycleStateUpdate, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable updateInstanceOverride(InstanceOverrideStateUpdate overrideStateUpdate) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_UPDATE_INSTANCE_OVERRIDE_STATE, overrideStateUpdate, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<AgentChangeEvent> observeAgents() {
        return Observable.create(emitter -> {
            StreamObserver<AgentChangeEvent> streamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = callStreaming(METHOD_OBSERVE_AGENTS, Empty.getDefaultInstance(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);
    }

    private <ReqT, RespT> ClientCall call(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.call(sessionContext, client, methodDescriptor, request, configuration.getRequestTimeout(), responseObserver);
    }

    private <ReqT, RespT> ClientCall callStreaming(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.callStreaming(sessionContext, client, methodDescriptor, request, responseObserver);
    }

    private Completable toCompletable(Action1<Emitter<Empty>> emitter) {
        return toObservable(emitter).toCompletable();
    }

    private <T> Observable<T> toObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        ).timeout(getRxJavaAdjustedTimeout(configuration.getRequestTimeout()), TimeUnit.MILLISECONDS);
    }
}
