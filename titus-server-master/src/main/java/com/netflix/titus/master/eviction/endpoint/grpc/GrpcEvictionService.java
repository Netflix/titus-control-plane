package com.netflix.titus.master.eviction.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.Reference;
import com.netflix.titus.grpc.protogen.SystemDisruptionBudget;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toCoreTier;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcEvictionQuota;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcSystemDisruptionBudget;

@Singleton
public class GrpcEvictionService extends EvictionServiceGrpc.EvictionServiceImplBase {

    private final EvictionOperations evictionOperations;

    @Inject
    public GrpcEvictionService(EvictionOperations evictionOperations) {
        this.evictionOperations = evictionOperations;
    }

    @Override
    public void getDisruptionBudget(Reference request, StreamObserver<SystemDisruptionBudget> responseObserver) {
        com.netflix.titus.api.eviction.model.SystemDisruptionBudget budget;
        switch (request.getReferenceCase()) {
            case GLOBAL:
                budget = evictionOperations.getGlobalDisruptionBudget();
                break;
            case TIER:
                budget = evictionOperations.getTierDisruptionBudget(toCoreTier(request.getTier()));
                break;
            case CAPACITYGROUP:
                budget = evictionOperations.getCapacityGroupDisruptionBudget(request.getCapacityGroup());
                break;
            case JOBID:
            case TASKID:
            default:
                responseObserver.onError(new IllegalArgumentException("Reference type not supported: " + request.getReferenceCase()));
                return;
        }

        responseObserver.onNext(toGrpcSystemDisruptionBudget(budget));
        responseObserver.onCompleted();
    }

    @Override
    public void getEvictionQuota(Reference request, StreamObserver<EvictionQuota> responseObserver) {
        com.netflix.titus.api.eviction.model.EvictionQuota evictionQuota;
        switch (request.getReferenceCase()) {
            case GLOBAL:
                evictionQuota = evictionOperations.getGlobalEvictionQuota();
                break;
            case TIER:
                evictionQuota = evictionOperations.getTierEvictionQuota(toCoreTier(request.getTier()));
                break;
            case CAPACITYGROUP:
                evictionQuota = evictionOperations.getCapacityGroupEvictionQuota(request.getCapacityGroup());
                break;
            case JOBID:
            case TASKID:
            default:
                responseObserver.onError(new IllegalArgumentException("Reference type not supported: " + request.getReferenceCase()));
                return;
        }

        responseObserver.onNext(toGrpcEvictionQuota(evictionQuota));
        responseObserver.onCompleted();
    }

    @Override
    public void terminateTask(TaskTerminateRequest request, StreamObserver<TaskTerminateResponse> responseObserver) {
        evictionOperations.terminateTask(request.getTaskId(), request.getReason()).subscribe(
                () -> {
                    responseObserver.onNext(TaskTerminateResponse.newBuilder()
                            .setAllowed(true)
                            .setReasonCode("normal")
                            .setReasonMessage("Terminating")
                            .build()
                    );
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void observeEvents(ObserverEventRequest request, StreamObserver<EvictionServiceEvent> responseObserver) {
        Subscription subscription = evictionOperations.events(request.getIncludeSnapshot()).subscribe(
                next -> responseObserver.onNext(GrpcEvictionModelConverters.toGrpcEvent(next)),
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("Eviction event stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );
        ServerCallStreamObserver<EvictionServiceEvent> serverObserver = (ServerCallStreamObserver<EvictionServiceEvent>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }
}
