/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.kube;

import java.util.Set;

import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;

public class EmbeddedKubeClusters {

    public static String SERVER_GROUP_CRITICAL = "critical1";
    public static String SERVER_GROUP_ELASTIC = "flex1";
    public static String SERVER_GROUP_GPU = "flexGpu";

    public static String RESOURCE_POOL_ELASTIC = "elastic";
    public static String RESOURCE_POOL_RESERVED = "reserved";
    public static String RESOURCE_POOL_GPU = "gpu";

    public static EmbeddedKubeCluster basicCluster(int desired) {
        EmbeddedKubeCluster embedded = new EmbeddedKubeCluster();
        embedded.addServerGroup(newReservedServerGroup(desired, AwsInstanceType.M3_XLARGE, null));
        embedded.addServerGroup(newElasticServerGroup(desired, AwsInstanceType.M3_2XLARGE, null));
        embedded.addServerGroup(newGpuServerGroup(desired, AwsInstanceType.G2_2XLarge, null));
        return embedded;
    }


    public static EmbeddedKubeCluster basicClusterWithCustomZones(int desired, String[] zones) {
        Set<String> zoneSet = CollectionsExt.asSet(zones);
        EmbeddedKubeCluster embedded = new EmbeddedKubeCluster();
        embedded.addServerGroup(newReservedServerGroup(desired, AwsInstanceType.M3_XLARGE, zoneSet));
        embedded.addServerGroup(newElasticServerGroup(desired, AwsInstanceType.M3_2XLARGE, zoneSet));
        embedded.addServerGroup(newGpuServerGroup(desired, AwsInstanceType.G2_2XLarge, zoneSet));
        return embedded;
    }

    public static EmbeddedKubeCluster basicClusterWithLargeInstances(int desired) {
        EmbeddedKubeCluster embedded = new EmbeddedKubeCluster();
        embedded.addServerGroup(newReservedServerGroup(desired, AwsInstanceType.M5_Metal, null));
        embedded.addServerGroup(newElasticServerGroup(desired, AwsInstanceType.R5_Metal, null));
        embedded.addServerGroup(newGpuServerGroup(desired, AwsInstanceType.P3_16XLarge, null));
        return embedded;
    }

    private static EmbeddedServerGroup newReservedServerGroup(int desired, AwsInstanceType instanceType, Set<String> zones) {
        return EmbeddedServerGroup.newBuilder()
                .withName(SERVER_GROUP_CRITICAL)
                .withInstanceType(instanceType.name())
                .withResourcePool(RESOURCE_POOL_RESERVED)
                .withSize(desired)
                .withZones(zones)
                .build();
    }

    private static EmbeddedServerGroup newElasticServerGroup(int desired, AwsInstanceType instanceType, Set<String> zones) {
        return EmbeddedServerGroup.newBuilder()
                .withName(SERVER_GROUP_ELASTIC)
                .withInstanceType(instanceType.name())
                .withResourcePool(RESOURCE_POOL_ELASTIC)
                .withSize(desired)
                .withZones(zones)
                .build();
    }

    private static EmbeddedServerGroup newGpuServerGroup(int desired, AwsInstanceType instanceType, Set<String> zones) {
        return EmbeddedServerGroup.newBuilder()
                .withName(SERVER_GROUP_GPU)
                .withInstanceType(instanceType.name())
                .withResourcePool(RESOURCE_POOL_GPU)
                .withSize(desired)
                .withZones(zones)
                .build();
    }
}
