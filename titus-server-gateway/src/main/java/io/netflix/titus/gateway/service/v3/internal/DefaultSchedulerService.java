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

package io.netflix.titus.gateway.service.v3.internal;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.SchedulerServiceStub;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectorId;
import com.netflix.titus.grpc.protogen.SystemSelectorUpdate;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.gateway.service.v3.SchedulerService;
import io.netflix.titus.runtime.endpoint.v3.grpc.GrpcSchedulerModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;
import static io.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestCompletable;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createSimpleStreamObserver;
import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultSchedulerService implements SchedulerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultSchedulerService.class);

    private final GrpcClientConfiguration configuration;
    private final SchedulerServiceStub client;
    private final SessionContext sessionContext;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public DefaultSchedulerService(GrpcClientConfiguration configuration,
                                   SchedulerServiceStub client,
                                   SessionContext sessionContext,
                                   @Named(SCHEDULER_SANITIZER) EntitySanitizer entitySanitizer) {
        this.configuration = configuration;
        this.client = client;
        this.sessionContext = sessionContext;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<SystemSelectors> getSystemSelectors() {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<SystemSelectors> streamObserver = createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getSystemSelectors(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SystemSelector> getSystemSelector(String id) {
        return createRequestObservable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<SystemSelector> streamObserver = createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getSystemSelector(SystemSelectorId.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable createSystemSelector(SystemSelector systemSelector) {
        io.netflix.titus.api.scheduler.model.SystemSelector coreSystemSelector = GrpcSchedulerModelConverters.toCoreSystemSelector(systemSelector);
        Set<ConstraintViolation<io.netflix.titus.api.scheduler.model.SystemSelector>> violations = entitySanitizer.validate(coreSystemSelector);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).createSystemSelector(systemSelector, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateSystemSelector(String id, SystemSelector systemSelector) {
        io.netflix.titus.api.scheduler.model.SystemSelector coreSystemSelector = GrpcSchedulerModelConverters.toCoreSystemSelector(systemSelector);
        Set<ConstraintViolation<io.netflix.titus.api.scheduler.model.SystemSelector>> violations = entitySanitizer.validate(coreSystemSelector);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }

        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = createSimpleStreamObserver(emitter);
            SystemSelectorUpdate systemSelectorUpdate = SystemSelectorUpdate.newBuilder().setId(id).setSystemSelector(systemSelector).build();
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).updateSystemSelector(systemSelectorUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return createRequestCompletable(emitter -> {
            attachCancellingCallback(emitter);
            StreamObserver<Empty> streamObserver = createSimpleStreamObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).deleteSystemSelector(SystemSelectorId.newBuilder().setId(id).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }
}
