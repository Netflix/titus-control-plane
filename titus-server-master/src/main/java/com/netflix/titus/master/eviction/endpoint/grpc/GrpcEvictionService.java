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

package com.netflix.titus.master.eviction.endpoint.grpc;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.Reference;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import reactor.core.Disposable;

import static com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils.execute;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toCoreReference;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcEvent;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcEvictionQuota;

@Singleton
public class GrpcEvictionService extends EvictionServiceGrpc.EvictionServiceImplBase {

    private final EvictionOperations evictionOperations;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public GrpcEvictionService(EvictionOperations evictionOperations,
                               CallMetadataResolver callMetadataResolver) {
        this.evictionOperations = evictionOperations;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void getEvictionQuota(Reference request, StreamObserver<EvictionQuota> responseObserver) {
        com.netflix.titus.api.model.reference.Reference coreReference = toCoreReference(request);

        EvictionQuota evictionQuota;
        switch (request.getReferenceCase()) {
            case SYSTEM:
            case TIER:
            case CAPACITYGROUP:
                evictionQuota = toGrpcEvictionQuota(evictionOperations.getEvictionQuota(coreReference));
                break;
            case JOBID:
                Optional<EvictionQuota> jobQuotaOpt = evictionOperations.findEvictionQuota(coreReference).map(GrpcEvictionModelConverters::toGrpcEvictionQuota);
                if (!jobQuotaOpt.isPresent()) {
                    responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Job not found or no eviction quota associated with the job")));
                    return;
                }
                evictionQuota = jobQuotaOpt.get();
                break;
            case TASKID:
                Optional<EvictionQuota> taskQuotaOpt = evictionOperations.findEvictionQuota(coreReference).map(GrpcEvictionModelConverters::toGrpcEvictionQuota);
                if (!taskQuotaOpt.isPresent()) {
                    responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Task not found or no eviction quota associated with the job")));
                    return;
                }
                evictionQuota = taskQuotaOpt.get();
                break;
            default:
                responseObserver.onError(new IllegalArgumentException("Reference type not supported: " + request.getReferenceCase()));
                return;
        }

        responseObserver.onNext(evictionQuota);
        responseObserver.onCompleted();
    }

    @Override
    public void terminateTask(TaskTerminateRequest request, StreamObserver<TaskTerminateResponse> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            evictionOperations.terminateTask(request.getTaskId(), request.getReason(), CallMetadataUtils.toCallerId(callMetadata)).subscribe(
                    next -> {
                    },
                    responseObserver::onError,
                    () -> {
                        responseObserver.onNext(TaskTerminateResponse.newBuilder()
                                .setAllowed(true)
                                .setReasonCode("normal")
                                .setReasonMessage("Terminating")
                                .build()
                        );
                        responseObserver.onCompleted();
                    }
            );

        });
    }

    @Override
    public void observeEvents(ObserverEventRequest request, StreamObserver<EvictionServiceEvent> responseObserver) {
        Disposable subscription = evictionOperations.events(request.getIncludeSnapshot()).subscribe(
                next -> toGrpcEvent(next).ifPresent(responseObserver::onNext),
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("Eviction event stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );
        ServerCallStreamObserver<EvictionServiceEvent> serverObserver = (ServerCallStreamObserver<EvictionServiceEvent>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::dispose);
    }

    private EvictionQuota toVeryHighQuota(Reference reference) {
        return EvictionQuota.newBuilder()
                .setTarget(reference)
                .setQuota(ReadOnlyEvictionOperations.VERY_HIGH_QUOTA)
                .build();
    }
}
