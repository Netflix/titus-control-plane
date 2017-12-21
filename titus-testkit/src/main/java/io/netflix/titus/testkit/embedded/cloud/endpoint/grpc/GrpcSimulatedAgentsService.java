package io.netflix.titus.testkit.embedded.cloud.endpoint.grpc;

import java.util.HashSet;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceImplBase;
import com.netflix.titus.simulator.TitusCloudSimulator;
import com.netflix.titus.simulator.TitusCloudSimulator.CapacityUpdateRequest;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudGateway;

@Singleton
public class GrpcSimulatedAgentsService extends SimulatedAgentServiceImplBase {

    private final SimulatedCloudGateway gateway;

    @Inject
    public GrpcSimulatedAgentsService(SimulatedCloudGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public void getAllInstanceGroups(Empty request, StreamObserver<SimulatedInstanceGroup> responseObserver) {
        gateway.getAllInstanceGroups().forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getInstanceGroups(TitusCloudSimulator.Ids ids, StreamObserver<SimulatedInstanceGroup> responseObserver) {
        gateway.getInstanceGroups(new HashSet<>(ids.getIdsList())).forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getInstancesOfInstanceGroup(TitusCloudSimulator.Id request, StreamObserver<TitusCloudSimulator.SimulatedInstance> responseObserver) {
        gateway.getInstances(request.getId()).forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getInstances(TitusCloudSimulator.Ids request, StreamObserver<TitusCloudSimulator.SimulatedInstance> responseObserver) {
        request.getIdsList().forEach(id -> responseObserver.onNext(gateway.getInstance(id)));
        responseObserver.onCompleted();
    }

    @Override
    public void updateCapacity(CapacityUpdateRequest request, StreamObserver<Empty> responseObserver) {
        gateway.updateCapacity(request.getInstanceGroupId(), request.getCapacity());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void terminateAndShrinkInstance(TitusCloudSimulator.Id request, StreamObserver<Empty> responseObserver) {
        gateway.terminateInstance(request.getId(), true);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void terminateInstance(TitusCloudSimulator.Id instanceId, StreamObserver<Empty> responseObserver) {
        gateway.terminateInstance(instanceId.getId(), false);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
