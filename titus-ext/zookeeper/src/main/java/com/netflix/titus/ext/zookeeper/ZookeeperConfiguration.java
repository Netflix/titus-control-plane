package com.netflix.titus.ext.zookeeper;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusMaster.ext.zookeeper")
public interface ZookeeperConfiguration {

    @DefaultValue("localhost:2181")
    String getZkConnectionString();

    @DefaultValue("10000")
    int getZkConnectionTimeoutMs();

    @DefaultValue("500")
    int getZkConnectionRetrySleepMs();

    @DefaultValue("5")
    int getZkConnectionMaxRetries();

    @DefaultValue("/titus")
    String getZkRoot();
}
