package com.netflix.titus.runtime.connector.titusmaster;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.masterClient")
public interface TitusMasterClientConfiguration {

    @DefaultValue("http")
    String getMasterScheme();

    @DefaultValue("127.0.0.1")
    String getMasterIp();

    @DefaultValue("7001")
    int getMasterHttpPort();

    @DefaultValue("7104")
    int getMasterGrpcPort();
}
