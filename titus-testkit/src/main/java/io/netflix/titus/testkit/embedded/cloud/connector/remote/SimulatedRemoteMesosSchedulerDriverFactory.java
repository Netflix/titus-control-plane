package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

@Singleton
public class SimulatedRemoteMesosSchedulerDriverFactory implements MesosSchedulerDriverFactory {

    private final CloudSimulatorConnectorConfiguration configuration;
    private final ManagedChannel channel;

    @Inject
    public SimulatedRemoteMesosSchedulerDriverFactory(CloudSimulatorConnectorConfiguration configuration) {
        this.configuration = configuration;
        this.channel = ManagedChannelBuilder.forAddress(configuration.getHost(), configuration.getGrpcPort())
                .usePlaintext(true)
                .build();
    }

    @PreDestroy
    public void shutdown() {
        channel.shutdownNow();
    }

    @Override
    public SchedulerDriver createDriver(Protos.FrameworkInfo framework, String mesosMaster, Scheduler scheduler) {
        return new SimulatedRemoteSchedulerDriver(configuration, channel, scheduler);
    }
}
