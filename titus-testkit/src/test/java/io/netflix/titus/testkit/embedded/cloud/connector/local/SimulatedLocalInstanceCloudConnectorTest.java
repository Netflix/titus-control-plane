package io.netflix.titus.testkit.embedded.cloud.connector.local;

import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedInstanceCloudConnectorTest;

public class SimulatedLocalInstanceCloudConnectorTest extends AbstractSimulatedInstanceCloudConnectorTest {

    @Override
    protected InstanceCloudConnector setup(SimulatedCloud cloud) {
        return new SimulatedLocalInstanceCloudConnector(cloud);
    }
}