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

package com.netflix.titus.gateway.eviction;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.Reference;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toCoreReference;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcEvent;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcEvictionQuota;

@Singleton
public class GatewayGrpcEvictionService extends EvictionServiceGrpc.EvictionServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(GatewayGrpcEvictionService.class);

    private final EvictionServiceClient evictionServiceClient;

    @Inject
    public GatewayGrpcEvictionService(EvictionServiceClient evictionServiceClient) {
        this.evictionServiceClient = evictionServiceClient;
    }

    @Override
    public void getEvictionQuota(Reference request, StreamObserver<EvictionQuota> responseObserver) {
        Disposable disposable = evictionServiceClient.getEvictionQuota(toCoreReference(request)).subscribe(
                next -> responseObserver.onNext(toGrpcEvictionQuota(next)),
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, disposable);
    }

    @Override
    public void terminateTask(TaskTerminateRequest request, StreamObserver<TaskTerminateResponse> responseObserver) {
        Disposable disposable = evictionServiceClient.terminateTask(request.getTaskId(), request.getReason())
                .subscribe(
                        next -> {
                        },
                        e -> {
                            if (e instanceof EvictionException) {
                                // TODO Improve error reporting
                                responseObserver.onNext(TaskTerminateResponse.newBuilder()
                                        .setAllowed(true)
                                        .setReasonCode("failure")
                                        .setReasonMessage(e.getMessage())
                                        .build()
                                );
                            } else {
                                safeOnError(logger, e, responseObserver);
                            }
                        },
                        () -> {
                            responseObserver.onNext(TaskTerminateResponse.newBuilder().setAllowed(true).build());
                            responseObserver.onCompleted();
                        }
                );
        attachCancellingCallback(responseObserver, disposable);
    }

    @Override
    public void observeEvents(ObserverEventRequest request, StreamObserver<EvictionServiceEvent> responseObserver) {
        Disposable disposable = evictionServiceClient.observeEvents(request.getIncludeSnapshot()).subscribe(
                event -> toGrpcEvent(event).ifPresent(responseObserver::onNext),
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, disposable);
    }
}
