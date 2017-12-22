package io.netflix.titus.testkit.embedded.cloud;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import io.netflix.titus.common.aws.AwsInstanceType;

import static io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor.awsInstanceGroup;

public class SimulatedCloudRunner {

    public static void main(String[] args) throws InterruptedException {
        LifecycleInjector injector = InjectorBuilder.fromModule(new EmbeddedCloudModule()).createInjector();

        SimulatedCloud simulatedCloud = injector.getInstance(SimulatedCloud.class);

        simulatedCloud.createAgentInstanceGroups(
                awsInstanceGroup("critical1", AwsInstanceType.M3_XLARGE, 5),
                awsInstanceGroup("flex1", AwsInstanceType.M3_2XLARGE, 5),
                awsInstanceGroup("flexGpu", AwsInstanceType.G2_2XLarge, 5)
        );

        injector.awaitTermination();
    }
}
