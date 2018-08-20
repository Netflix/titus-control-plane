package com.netflix.titus.ext.zookeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.MasterDescription;

import static org.mockito.Mockito.when;

public class ZookeeperTestUtils {

    public static ZookeeperConfiguration withEmbeddedZookeeper(ZookeeperConfiguration configurationMock, String zkConnectStr) {
        when(configurationMock.getZkConnectionTimeoutMs()).thenReturn(1000);
        when(configurationMock.getZkConnectionRetrySleepMs()).thenReturn(100);
        when(configurationMock.getZkConnectionMaxRetries()).thenReturn(3);
        when(configurationMock.getZkConnectionString()).thenReturn(zkConnectStr);
        return configurationMock;
    }

    public static MasterDescription newMasterDescription(int apiPort) {
        return new MasterDescription(
                getHost(),
                getHostIP(),
                apiPort,
                "http://myFakeStatusURI",
                System.currentTimeMillis()
        );
    }

    public static MasterInstance newMasterInstance(String instanceId, MasterState initialState) {
        return MasterInstance.newBuilder()
                .withInstanceId(instanceId)
                .withIpAddress("1.2.3.4")
                .withStatus(MasterStatus.newBuilder()
                        .withState(initialState)
                        .withReasonCode("test")
                        .withReasonMessage("Initial MasterInstance version")
                        .build()
                )
                .withStatusHistory(Collections.emptyList())
                .build();
    }

    private static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }

    private static String getHostIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }
}
