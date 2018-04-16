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

package com.netflix.titus.master.scheduler.endpoint.grpc;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.scheduler.service.SchedulerService;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectorId;
import com.netflix.titus.grpc.protogen.SystemSelectorUpdate;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcSchedulerModelConverters;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultSchedulerServiceGrpc extends SchedulerServiceGrpc.SchedulerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulerServiceGrpc.class);

    private final SchedulerService schedulerService;
    private final CallMetadataResolver sessionContext;

    @Inject
    public DefaultSchedulerServiceGrpc(SchedulerService schedulerService, CallMetadataResolver sessionContext) {
        this.schedulerService = schedulerService;
        this.sessionContext = sessionContext;
    }

    @Override
    public void getSystemSelectors(Empty request, StreamObserver<SystemSelectors> responseObserver) {
        execute(responseObserver, user -> {
            List<SystemSelector> all = schedulerService.getSystemSelectors().stream()
                    .map(GrpcSchedulerModelConverters::toGrpcSystemSelector)
                    .collect(Collectors.toList());
            responseObserver.onNext(SystemSelectors.newBuilder().addAllSystemSelectors(all).build());
        });
    }

    @Override
    public void getSystemSelector(SystemSelectorId request, StreamObserver<SystemSelector> responseObserver) {
        execute(responseObserver, user -> {
            com.netflix.titus.api.scheduler.model.SystemSelector systemSelector = schedulerService.getSystemSelector(request.getId());
            SystemSelector grpcSystemSelector = GrpcSchedulerModelConverters.toGrpcSystemSelector(systemSelector);
            responseObserver.onNext(grpcSystemSelector);
        });
    }

    @Override
    public void createSystemSelector(SystemSelector request, StreamObserver<Empty> responseObserver) {
        schedulerService.createSystemSelector(
                GrpcSchedulerModelConverters.toCoreSystemSelector(request)
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void updateSystemSelector(SystemSelectorUpdate request, StreamObserver<Empty> responseObserver) {
        schedulerService.updateSystemSelector(
                request.getId(),
                GrpcSchedulerModelConverters.toCoreSystemSelector(request.getSystemSelector())
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void deleteSystemSelector(SystemSelectorId request, StreamObserver<Empty> responseObserver) {
        schedulerService.deleteSystemSelector(request.getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    private void execute(StreamObserver<?> responseObserver, Consumer<CallMetadata> action) {
        Optional<CallMetadata> callMetadata = sessionContext.resolve();
        if (!callMetadata.isPresent()) {
            responseObserver.onError(TitusServiceException.noCallerId());
            return;
        }
        try {
            action.accept(callMetadata.get());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
