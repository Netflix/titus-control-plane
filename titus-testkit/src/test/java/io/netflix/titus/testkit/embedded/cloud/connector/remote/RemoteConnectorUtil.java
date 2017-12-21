package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import com.google.inject.AbstractModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloudConfiguration;
import io.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudEndpointModule;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

class RemoteConnectorUtil {

    static LifecycleInjector createSimulatedCloudGrpcServer(SimulatedCloud cloud, int grpcPort) {
        return InjectorBuilder.fromModules(
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(SimulatedCloud.class).toInstance(cloud);
                        bind(SimulatedCloudConfiguration.class).toInstance(newConfiguration(grpcPort));
                    }
                },
                new SimulatedCloudEndpointModule()
        ).createInjector();
    }

    static SimulatedCloudConfiguration newConfiguration(int grpcPort) {
        SimulatedCloudConfiguration mock = Mockito.mock(SimulatedCloudConfiguration.class);
        when(mock.getGrpcPort()).thenReturn(grpcPort);
        return mock;
    }

    static CloudSimulatorConnectorConfiguration newConnectorConfiguration(int grpcPort) {
        CloudSimulatorConnectorConfiguration mock = Mockito.mock(CloudSimulatorConnectorConfiguration.class);
        when(mock.getHost()).thenReturn("localhost");
        when(mock.getGrpcPort()).thenReturn(grpcPort);
        return mock;
    }
}
