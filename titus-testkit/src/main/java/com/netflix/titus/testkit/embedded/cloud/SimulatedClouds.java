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

package com.netflix.titus.testkit.embedded.cloud;

import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;

/**
 * A collection of pre-configured cloud setups.
 */
public final class SimulatedClouds {

    public static SimulatedCloud basicCloud(int desired) {
        SimulatedCloud simulatedCloud = new SimulatedCloud();
        simulatedCloud.createAgentInstanceGroups(
                SimulatedAgentGroupDescriptor.awsInstanceGroup("critical1", AwsInstanceType.M3_XLARGE, desired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flex1", AwsInstanceType.M3_2XLARGE, desired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flexGpu", AwsInstanceType.G2_2XLarge, desired)
        );
        return simulatedCloud;
    }

    public static SimulatedCloud basicCloudWithLargeInstances(int desired) {
        SimulatedCloud simulatedCloud = new SimulatedCloud();
        simulatedCloud.createAgentInstanceGroups(
                SimulatedAgentGroupDescriptor.awsInstanceGroup("critical1", AwsInstanceType.M5_Metal, desired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flex1", AwsInstanceType.R5_Metal, desired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flexGpu", AwsInstanceType.P3_16XLarge, desired)
        );
        return simulatedCloud;
    }

    public static SimulatedCloud twoPartitionsPerTierStack(int partitionDesired) {
        SimulatedCloud simulatedCloud = new SimulatedCloud();
        simulatedCloud.createAgentInstanceGroups(
                SimulatedAgentGroupDescriptor.awsInstanceGroup("critical1", AwsInstanceType.M3_XLARGE, partitionDesired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("critical2", AwsInstanceType.M4_XLarge, partitionDesired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flex1", AwsInstanceType.M3_2XLARGE, partitionDesired),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flex2", AwsInstanceType.M4_2XLarge, partitionDesired)
        );
        return simulatedCloud;
    }
}
