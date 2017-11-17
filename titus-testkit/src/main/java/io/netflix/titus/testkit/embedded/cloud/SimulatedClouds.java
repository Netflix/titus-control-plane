package io.netflix.titus.testkit.embedded.cloud;

import io.netflix.titus.common.aws.AwsInstanceType;

import static io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor.awsInstanceGroup;

/**
 * A collection of pre-configured cloud setups.
 */
public final class SimulatedClouds {

    public static SimulatedCloud basicCloud(int desired) {
        SimulatedCloud simulatedCloud = new SimulatedCloud();
        simulatedCloud.createAgentInstanceGroups(
                awsInstanceGroup("critical1", AwsInstanceType.M3_XLARGE, desired),
                awsInstanceGroup("flex1", AwsInstanceType.M3_2XLARGE, desired),
                awsInstanceGroup("flexGpu", AwsInstanceType.G2_2XLarge, desired)
        );
        return simulatedCloud;
    }

    public static SimulatedCloud twoPartitionsPerTierStack(int partitionDesired) {
        SimulatedCloud simulatedCloud = new SimulatedCloud();
        simulatedCloud.createAgentInstanceGroups(
                awsInstanceGroup("critical1", AwsInstanceType.M3_XLARGE, partitionDesired),
                awsInstanceGroup("critical2", AwsInstanceType.M4_XLarge, partitionDesired),
                awsInstanceGroup("flex1", AwsInstanceType.M3_2XLARGE, partitionDesired),
                awsInstanceGroup("flex2", AwsInstanceType.M4_2XLarge, partitionDesired)
        );
        return simulatedCloud;
    }
}
