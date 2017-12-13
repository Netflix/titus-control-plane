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

import java.util.Arrays;

import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.master.config.MasterConfiguration;

import static org.mockito.Mockito.when;

/**
 * Provides pre-initialized {@link MasterConfiguration} objects.
 */
public class ConfigurationMockSamples {

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
        when(mock.getZkRoot()).thenReturn("/titus/master");
        return mock;
    }

    public static JobConfiguration withJobConfiguration(JobConfiguration mock) {
        when(mock.getDefaultSecurityGroups()).thenReturn(Arrays.asList("infrastructure", "persistence"));
        when(mock.getDefaultRuntimeLimitSec()).thenReturn(432000L);
        when(mock.getMaxRuntimeLimitSec()).thenReturn(864000L);
        return mock;
    }

    public static MasterConfiguration withJobSpec(MasterConfiguration mock) {
        when(mock.getMaxCPUs()).thenReturn(32);
        when(mock.getMaxMemory()).thenReturn(244000);
        when(mock.getMaxDisk()).thenReturn(640000);
        when(mock.getMaxNetworkMbps()).thenReturn(6000);
        when(mock.getMaxBatchInstances()).thenReturn(10000);
        when(mock.getMaxServiceInstances()).thenReturn(1000);
        return mock;
    }
}
