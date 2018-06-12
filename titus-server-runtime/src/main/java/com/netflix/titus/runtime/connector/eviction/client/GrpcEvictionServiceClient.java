package com.netflix.titus.runtime.connector.eviction.client;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceStub;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcReference;

@Singleton
public class GrpcEvictionServiceClient implements EvictionServiceClient {

    private final GrpcClientConfiguration configuration;
    private final EvictionServiceStub client;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public GrpcEvictionServiceClient(GrpcClientConfiguration configuration,
                                     EvictionServiceStub client,
                                     CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Observable<SystemDisruptionBudget> getDisruptionBudget(com.netflix.titus.api.model.Reference reference) {
        return GrpcUtil.<com.netflix.titus.grpc.protogen.SystemDisruptionBudget>createRequestObservable(emitter -> {
                    StreamObserver<com.netflix.titus.grpc.protogen.SystemDisruptionBudget> streamObserver = createSimpleClientResponseObserver(emitter);
                    createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getDisruptionBudget(toGrpcReference(reference), streamObserver);
                },
                configuration.getRequestTimeout()
        ).map(GrpcEvictionModelConverters::toCoreSystemDisruptionBudget);
    }

    @Override
    public Observable<EvictionQuota> getEvictionQuota(com.netflix.titus.api.model.Reference reference) {
        return GrpcUtil.<com.netflix.titus.grpc.protogen.EvictionQuota>createRequestObservable(emitter -> {
                    StreamObserver<com.netflix.titus.grpc.protogen.EvictionQuota> streamObserver = createSimpleClientResponseObserver(emitter);
                    createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getEvictionQuota(toGrpcReference(reference), streamObserver);
                },
                configuration.getRequestTimeout()
        ).map(GrpcEvictionModelConverters::toCoreEvictionQuota);
    }

    @Override
    public Completable terminateTask(String taskId, String reason) {
        return GrpcUtil.<com.netflix.titus.grpc.protogen.TaskTerminateResponse>createRequestObservable(emitter -> {
                    StreamObserver<com.netflix.titus.grpc.protogen.TaskTerminateResponse> streamObserver = createSimpleClientResponseObserver(emitter);
                    TaskTerminateRequest request = TaskTerminateRequest.newBuilder()
                            .setTaskId(taskId)
                            .setReason(reason)
                            .build();
                    createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).terminateTask(request, streamObserver);
                },
                configuration.getRequestTimeout()
        ).flatMap(response -> {
            if (!response.getAllowed()) {
                // TODO Better error handling
                return Observable.error(new IllegalStateException(response.getReasonCode() + ": " + response.getReasonMessage()));
            }
            return Observable.empty();
        }).toCompletable();
    }

    @Override
    public Observable<EvictionEvent> observeEvents(boolean includeSnapshot) {
        return GrpcUtil.<EvictionServiceEvent>createRequestObservable(emitter -> {
            StreamObserver<EvictionServiceEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            ObserverEventRequest request = ObserverEventRequest.newBuilder()
                    .setIncludeSnapshot(includeSnapshot)
                    .build();
            createWrappedStub(client, callMetadataResolver).observeEvents(request, streamObserver);
        }).map(GrpcEvictionModelConverters::toCoreEvent);
    }
}
