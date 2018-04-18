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
import com.netflix.titus.gateway.service.v3.SchedulerService;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectorId;
import com.netflix.titus.grpc.protogen.SystemSelectorUpdate;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultSchedulerServiceGrpc extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulerServiceGrpc.class);

    private final SchedulerService schedulerService;

    @Inject
    public DefaultSchedulerServiceGrpc(SchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    @Override
    public void getSystemSelectors(Empty request, StreamObserver<SystemSelectors> responseObserver) {
        Subscription subscription = schedulerService.getSystemSelectors().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getSystemSelector(SystemSelectorId request, StreamObserver<SystemSelector> responseObserver) {
        Subscription subscription = schedulerService.getSystemSelector(request.getId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void createSystemSelector(SystemSelector request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = schedulerService.createSystemSelector(request).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void updateSystemSelector(SystemSelectorUpdate request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = schedulerService.updateSystemSelector(request.getId(), request.getSystemSelector()).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void deleteSystemSelector(SystemSelectorId request, StreamObserver<Empty> responseObserver) {
        Subscription subscription = schedulerService.deleteSystemSelector(request.getId()).subscribe(
                () -> emitEmptyReply(responseObserver),
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    private void emitEmptyReply(StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}