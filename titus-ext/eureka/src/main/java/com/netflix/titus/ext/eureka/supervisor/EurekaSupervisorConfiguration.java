package com.netflix.titus.ext.eureka.supervisor;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusMaster.ext.eureka.supervisor")
public interface EurekaSupervisorConfiguration {

    @DefaultValue("myInstanceId")
    String getInstanceId();
}
