/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master;

import io.netflix.titus.master.config.MasterConfiguration;

import static org.mockito.Mockito.when;

/**
 * Provides pre-initialized {@link MasterConfiguration} objects.
 */
public class ConfigurationMockSamples {

    public static final String agentClusterAttributeName = "cluster";

    public static MasterConfiguration withExecutionEnvironment(MasterConfiguration mock) {
        when(mock.getRegion()).thenReturn("us-east-1");
        return mock;
    }

    public static MasterConfiguration withEmbeddedZookeeper(MasterConfiguration mock, String zkConnectStr) {
        when(mock.isLocalMode()).thenReturn(true);
        when(mock.getZkConnectionTimeoutMs()).thenReturn(1000);
        when(mock.getZkConnectionRetrySleepMs()).thenReturn(100);
        when(mock.getZkConnectionMaxRetries()).thenReturn(3);
        when(mock.getZkConnectionString()).thenReturn(zkConnectStr);
        when(mock.getZkRoot()).thenReturn("/mantis/master");
        return mock;
    }

    public static MasterConfiguration withSecurityGroups(MasterConfiguration mock) {
        when(mock.getDefaultSecurityGroupsList()).thenReturn("infrastructure,persistence");
        return mock;
    }

    public static MasterConfiguration withJobSpec(MasterConfiguration mock) {
        when(mock.getMaxCPUs()).thenReturn(32);
        when(mock.getMaxMemory()).thenReturn(244000);
        when(mock.getMaxDisk()).thenReturn(640000);
        when(mock.getMaxNetworkMbps()).thenReturn(6000);
        when(mock.getMaxBatchInstances()).thenReturn(10000);
        when(mock.getMaxServiceInstances()).thenReturn(1000);
        when(mock.getDefaultRuntimeLimit()).thenReturn(432000L);
        when(mock.getMaxRuntimeLimit()).thenReturn(864000L);
        return mock;
    }

    public static MasterConfiguration withAutoScaleClusterInfo(MasterConfiguration mock) {
        when(mock.getActiveSlaveAttributeName()).thenReturn("cluster");
        when(mock.getAutoscaleByAttributeName()).thenReturn(agentClusterAttributeName);
        when(mock.getInstanceTypeAttributeName()).thenReturn("itype");
        return mock;
    }
}
