package io.netflix.titus.testkit.embedded.cloud.connector.local;

import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedMesosSchedulerDriverTest;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class SimulatedLocalMesosSchedulerDriverTest extends AbstractSimulatedMesosSchedulerDriverTest {

    @Override
    protected SchedulerDriver setup(SimulatedCloud cloud, Protos.FrameworkInfo framework, Scheduler callbackHandler) {
        return new SimulatedLocalMesosSchedulerDriver(cloud, framework, callbackHandler);
    }
}