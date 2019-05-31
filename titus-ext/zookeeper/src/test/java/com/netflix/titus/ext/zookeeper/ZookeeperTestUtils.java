/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.ext.zookeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.service.MasterDescription;

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
                        .withMessage("Initial MasterInstance version")
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
