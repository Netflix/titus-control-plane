package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "cloudSimulator")
public interface CloudSimulatorConnectorConfiguration {

    @DefaultValue("localhost")
    String getHost();

    @DefaultValue("7006")
    int getGrpcPort();
}
