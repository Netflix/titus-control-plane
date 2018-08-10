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
import com.netflix.titus.grpc.protogen.TitusSupervisorServiceGrpc;
import com.netflix.titus.master.supervisor.service.SupervisorOperations;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters.toGrpcMasterInstance;

@Singleton
public class DefaultTitusSupervisorServiceGrpc extends TitusSupervisorServiceGrpc.TitusSupervisorServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTitusSupervisorServiceGrpc.class);

    private final SupervisorOperations supervisorOperations;

    @Inject
    public DefaultTitusSupervisorServiceGrpc(SupervisorOperations supervisorOperations) {
        this.supervisorOperations = supervisorOperations;
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
    public void restartMasterInstance(MasterInstanceId request, StreamObserver<Empty> responseObserver) {
        super.restartMasterInstance(request, responseObserver);
    }

    @Override
    public void observeMasterInstances(Empty request, StreamObserver<SupervisorEvent> responseObserver) {
        super.observeMasterInstances(request, responseObserver);
    }
}
