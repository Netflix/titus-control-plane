package io.netflix.titus.master;

import java.util.Map;

import com.netflix.archaius.api.annotations.Configuration;

@Configuration(prefix = "titusMaster.runtime")
public interface TitusRuntimeConfiguration {

    Map<String, String> getFitActions();
}
