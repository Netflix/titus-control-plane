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
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.gateway.service.v3.SchedulerService;
import io.netflix.titus.runtime.endpoint.v3.grpc.GrpcSchedulerModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.getCreateSystemSelectorMethod;
import static com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.getDeleteSystemSelectorMethod;
import static com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.getGetSystemSelectorMethod;
import static com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.getGetSystemSelectorsMethod;
import static com.netflix.titus.grpc.protogen.SchedulerServiceGrpc.getUpdateSystemSelectorMethod;
import static io.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;
import static io.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestCompletable;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createSimpleStreamObserver;

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
            StreamObserver<SystemSelectors> simpleStreamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(getGetSystemSelectorsMethod(), Empty.getDefaultInstance(), simpleStreamObserver);
            attachCancellingCallback(emitter, clientCall);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SystemSelector> getSystemSelector(String id) {
        return createRequestObservable(emitter -> {
            StreamObserver<SystemSelector> simpleStreamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(getGetSystemSelectorMethod(), SystemSelectorId.newBuilder().setId(id).build(), simpleStreamObserver);
            attachCancellingCallback(emitter, clientCall);
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
            StreamObserver<Empty> simpleStreamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(getCreateSystemSelectorMethod(), systemSelector, simpleStreamObserver);
            attachCancellingCallback(emitter, clientCall);
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
            StreamObserver<Empty> simpleStreamObserver = createSimpleStreamObserver(emitter);
            SystemSelectorUpdate systemSelectorUpdate = SystemSelectorUpdate.newBuilder().setId(id).setSystemSelector(systemSelector).build();
            ClientCall clientCall = call(getUpdateSystemSelectorMethod(), systemSelectorUpdate, simpleStreamObserver);
            attachCancellingCallback(emitter, clientCall);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> simpleStreamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(getDeleteSystemSelectorMethod(), SystemSelectorId.newBuilder().setId(id).build(), simpleStreamObserver);
            attachCancellingCallback(emitter, clientCall);
        }, configuration.getRequestTimeout());
    }

    private <ReqT, RespT> ClientCall call(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.call(sessionContext, client, methodDescriptor, request, configuration.getRequestTimeout(), responseObserver);
    }
}
