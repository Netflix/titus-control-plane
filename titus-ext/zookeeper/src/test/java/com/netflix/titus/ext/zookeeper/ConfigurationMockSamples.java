package com.netflix.titus.ext.zookeeper;

import com.netflix.titus.master.config.MasterConfiguration;

import static org.mockito.Mockito.when;

public class ConfigurationMockSamples {

    public static MasterConfiguration withExecutionEnvironment(MasterConfiguration mock) {
        when(mock.getRegion()).thenReturn("us-east-1");
        return mock;
    }

    public static ZookeeperConfiguration withEmbeddedZookeeper(ZookeeperConfiguration mock, String zkConnectStr) {
        when(mock.getZkConnectionTimeoutMs()).thenReturn(1000);
        when(mock.getZkConnectionRetrySleepMs()).thenReturn(100);
        when(mock.getZkConnectionMaxRetries()).thenReturn(3);
        when(mock.getZkConnectionString()).thenReturn(zkConnectStr);
        return mock;
    }
}
