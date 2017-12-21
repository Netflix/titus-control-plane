package io.netflix.titus.testkit.embedded.cloud.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceImplBase;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc;
import io.netflix.titus.testkit.embedded.cloud.endpoint.grpc.GrpcSimulatedAgentsService;
import io.netflix.titus.testkit.embedded.cloud.endpoint.grpc.GrpcSimulatedMesosService;
import io.netflix.titus.testkit.embedded.cloud.endpoint.grpc.SimulatedCloudGrpcServer;

public class SimulatedCloudEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SimulatedAgentServiceImplBase.class).to(GrpcSimulatedAgentsService.class);
        bind(SimulatedMesosServiceGrpc.SimulatedMesosServiceImplBase.class).to(GrpcSimulatedMesosService.class);
        bind(SimulatedCloudGrpcServer.class).asEagerSingleton();
    }
}
