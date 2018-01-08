package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

@Singleton
public class SimulatedRemoteMesosSchedulerDriverFactory implements MesosSchedulerDriverFactory {

    private final ManagedChannel channel;
    private final Protos.MasterInfo masterInfo;

    @Inject
    public SimulatedRemoteMesosSchedulerDriverFactory(CloudSimulatorResolver cloudSimulatorResolver) {
        Pair<String, Integer> address = cloudSimulatorResolver.resolveGrpcEndpoint();
        String host = address.getLeft();
        int port = address.getRight();

        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setId("MasterId#Simulated")
                .setAddress(Protos.Address.newBuilder().setHostname(host).setPort(port))
                .setHostname(host)
                .setIp(0)
                .setPort(port)
                .setVersion("1.2.simulated")
                .build();

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();
    }

    @PreDestroy
    public void shutdown() {
        channel.shutdownNow();
    }

    @Override
    public SchedulerDriver createDriver(Protos.FrameworkInfo framework, String mesosMaster, Scheduler scheduler) {
        return new SimulatedRemoteMesosSchedulerDriver(masterInfo, channel, scheduler);
    }
}
