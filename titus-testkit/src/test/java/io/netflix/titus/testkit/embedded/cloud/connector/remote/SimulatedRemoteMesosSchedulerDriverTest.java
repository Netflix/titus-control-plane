package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import com.netflix.governator.LifecycleInjector;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedMesosSchedulerDriverTest;
import io.netflix.titus.testkit.util.NetworkExt;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.junit.After;

public class SimulatedRemoteMesosSchedulerDriverTest extends AbstractSimulatedMesosSchedulerDriverTest {

    private LifecycleInjector injector;

    private SimulatedRemoteMesosSchedulerDriverFactory factory;

    @Override
    protected SchedulerDriver setup(SimulatedCloud cloud, Protos.FrameworkInfo framework, Scheduler callbackHandler) {
        int grpcPort = NetworkExt.findUnusedPort();
        this.injector = RemoteConnectorUtil.createSimulatedCloudGrpcServer(cloud, grpcPort);

        this.factory = new SimulatedRemoteMesosSchedulerDriverFactory(RemoteConnectorUtil.newConnectorConfiguration(grpcPort));
        return factory.createDriver(framework, "N/A", callbackHandler);
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.shutdown();
        }
        if (injector != null) {
            injector.close();
        }
    }
}
