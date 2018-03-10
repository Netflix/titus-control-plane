package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import com.netflix.governator.LifecycleInjector;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedInstanceCloudConnectorTest;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.util.NetworkExt;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class SimulatedRemoteInstanceCloudConnectorTest extends AbstractSimulatedInstanceCloudConnectorTest {

    private LifecycleInjector injector;

    @Override
    protected InstanceCloudConnector setup(SimulatedCloud cloud) {
        int grpcPort = NetworkExt.findUnusedPort();
        this.injector = RemoteConnectorUtil.createSimulatedCloudGrpcServer(cloud, grpcPort);
        return new SimulatedRemoteInstanceCloudConnector(RemoteConnectorUtil.newConnectorConfiguration(grpcPort));
    }

    @After
    public void tearDown() {
        if (injector != null) {
            injector.close();
        }
    }
}