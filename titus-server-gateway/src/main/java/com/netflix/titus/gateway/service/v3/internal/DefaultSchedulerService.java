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

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.google.protobuf.Empty;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import com.netflix.titus.gateway.service.v3.SchedulerService;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceStub;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectorId;
import com.netflix.titus.grpc.protogen.SystemSelectorUpdate;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcSchedulerModelConverters;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createEmptyClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultSchedulerService implements SchedulerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultSchedulerService.class);

    private final GrpcClientConfiguration configuration;
    private final SchedulerServiceStub client;
    private final CallMetadataResolver callMetadataResolver;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public DefaultSchedulerService(GrpcClientConfiguration configuration,
                                   SchedulerServiceStub client,
                                   CallMetadataResolver callMetadataResolver,
                                   @Named(SCHEDULER_SANITIZER) EntitySanitizer entitySanitizer) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<SystemSelectors> getSystemSelectors() {
        return createRequestObservable(emitter -> {
            StreamObserver<SystemSelectors> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getSystemSelectors(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SystemSelector> getSystemSelector(String id) {
        return createRequestObservable(emitter -> {
            StreamObserver<SystemSelector> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getSystemSelector(SystemSelectorId.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable createSystemSelector(SystemSelector systemSelector) {
        com.netflix.titus.api.scheduler.model.SystemSelector coreSystemSelector = GrpcSchedulerModelConverters.toCoreSystemSelector(systemSelector);
        Set<ConstraintViolation<com.netflix.titus.api.scheduler.model.SystemSelector>> violations = entitySanitizer.validate(coreSystemSelector);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).createSystemSelector(systemSelector, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateSystemSelector(String id, SystemSelector systemSelector) {
        com.netflix.titus.api.scheduler.model.SystemSelector coreSystemSelector = GrpcSchedulerModelConverters.toCoreSystemSelector(systemSelector);
        Set<ConstraintViolation<com.netflix.titus.api.scheduler.model.SystemSelector>> violations = entitySanitizer.validate(coreSystemSelector);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = createEmptyClientResponseObserver(emitter);
            SystemSelectorUpdate systemSelectorUpdate = SystemSelectorUpdate.newBuilder().setId(id).setSystemSelector(systemSelector).build();
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateSystemSelector(systemSelectorUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).deleteSystemSelector(SystemSelectorId.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }
}
