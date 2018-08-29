package com.netflix.titus.runtime.connector.supervisor.client;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.grpc.protogen.MasterInstanceId;
import com.netflix.titus.grpc.protogen.MasterInstances;
import com.netflix.titus.grpc.protogen.SupervisorEvent;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.supervisor.SupervisorClient;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.*;

@Singleton
public class GrpcSupervisorClient implements SupervisorClient {

    private final GrpcClientConfiguration configuration;
    private final SupervisorServiceStub client;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public GrpcSupervisorClient(GrpcClientConfiguration configuration,
                                SupervisorServiceStub client,
                                CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Observable<MasterInstances> getMasterInstances() {
        return createRequestObservable(emitter -> {
            StreamObserver<MasterInstances> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).getMasterInstances(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<MasterInstance> getMasterInstance(String instanceId) {
        return createRequestObservable(emitter -> {
            StreamObserver<MasterInstance> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout())
                    .getMasterInstance(MasterInstanceId.newBuilder().setInstanceId(instanceId).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<SupervisorEvent> observeEvents() {
        return createRequestObservable(emitter -> {
            StreamObserver<SupervisorEvent> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeEvents(Empty.getDefaultInstance(), streamObserver);
        });
    }

    @Override
    public Completable stopBeingLeader() {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).stopBeingLeader(Empty.getDefaultInstance(), streamObserver);
        }, configuration.getRequestTimeout());
    }
}
