package io.netflix.titus.testkit.embedded.cloud;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.simulatedCloud")
public interface SimulatedCloudConfiguration {

    @DefaultValue("7006")
    int getGrpcPort();
}
