package io.netflix.titus.testkit.embedded.cloud.endpoint.grpc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceImplBase;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc.SimulatedMesosServiceImplBase;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.netflix.titus.gateway.endpoint.v3.grpc.TitusGatewayGrpcServer;
import io.netflix.titus.runtime.endpoint.common.grpc.interceptor.ErrorCatchingServerInterceptor;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloudConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SimulatedCloudGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(TitusGatewayGrpcServer.class);

    private final SimulatedCloudConfiguration configuration;
    private final SimulatedAgentServiceImplBase simulatedAgentsService;
    private final SimulatedMesosServiceImplBase simulatedMesosService;

    private final AtomicBoolean started = new AtomicBoolean();
    private Server server;

    @Inject
    public SimulatedCloudGrpcServer(SimulatedCloudConfiguration configuration,
                                    SimulatedAgentServiceImplBase simulatedAgentsService,
                                    SimulatedMesosServiceImplBase simulatedMesosService) {
        this.configuration = configuration;
        this.simulatedAgentsService = simulatedAgentsService;
        this.simulatedMesosService = simulatedMesosService;
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            ServerBuilder serverBuilder = ServerBuilder.forPort(configuration.getGrpcPort());
            serverBuilder
                    .addService(ServerInterceptors.intercept(simulatedAgentsService, createInterceptors()))
                    .addService(ServerInterceptors.intercept(simulatedMesosService, createInterceptors()));
            this.server = serverBuilder.build();

            logger.info("Starting gRPC server on port {}.", configuration.getGrpcPort());
            try {
                this.server.start();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            logger.info("Started gRPC server on port {}.", configuration.getGrpcPort());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!server.isShutdown()) {
            server.shutdownNow();
        }
    }

    private List<ServerInterceptor> createInterceptors() {
        return Collections.singletonList(new ErrorCatchingServerInterceptor());
    }
}
