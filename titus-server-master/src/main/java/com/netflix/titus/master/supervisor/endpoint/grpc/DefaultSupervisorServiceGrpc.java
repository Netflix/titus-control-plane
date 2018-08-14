package com.netflix.titus.master.supervisor.endpoint.grpc;

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.grpc.protogen.MasterInstanceId;
import com.netflix.titus.grpc.protogen.MasterInstances;
import com.netflix.titus.grpc.protogen.SupervisorEvent;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import com.netflix.titus.master.supervisor.service.SupervisorOperations;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters.toGrpcMasterInstance;
import static com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils.execute;

@Singleton
public class DefaultSupervisorServiceGrpc extends SupervisorServiceGrpc.SupervisorServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSupervisorServiceGrpc.class);

    private final SupervisorOperations supervisorOperations;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultSupervisorServiceGrpc(SupervisorOperations supervisorOperations,
                                        CallMetadataResolver callMetadataResolver) {
        this.supervisorOperations = supervisorOperations;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void getMasterInstances(Empty request, StreamObserver<MasterInstances> responseObserver) {
        try {
            List<MasterInstance> instances = supervisorOperations.getMasterInstances().stream()
                    .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                    .collect(Collectors.toList());
            responseObserver.onNext(MasterInstances.newBuilder().addAllInstances(instances).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            GrpcUtil.safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void getMasterInstance(MasterInstanceId request, StreamObserver<MasterInstance> responseObserver) {
        try {
            responseObserver.onNext(toGrpcMasterInstance(supervisorOperations.getMasterInstance(request.getInstanceId())));
            responseObserver.onCompleted();
        } catch (Exception e) {
            GrpcUtil.safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void restartMasterInstance(Empty request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            try {
                supervisorOperations.restartMasterInstance(callMetadata);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        });
    }

    @Override
    public void observeEvents(Empty request, StreamObserver<SupervisorEvent> responseObserver) {
        Subscription subscription = supervisorOperations.events()
                .map(SupervisorGrpcModelConverters::toGrpcEvent)
                .subscribe(
                        responseObserver::onNext,
                        e -> responseObserver.onError(
                                new StatusRuntimeException(Status.INTERNAL
                                        .withDescription("Supervisor event stream terminated with an error")
                                        .withCause(e))
                        ),
                        responseObserver::onCompleted
                );
        ServerCallStreamObserver<SupervisorEvent> serverObserver = (ServerCallStreamObserver<SupervisorEvent>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }
}
