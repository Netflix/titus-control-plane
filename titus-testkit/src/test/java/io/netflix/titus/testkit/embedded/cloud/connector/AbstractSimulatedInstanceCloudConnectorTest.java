package io.netflix.titus.testkit.embedded.cloud.connector;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSimulatedInstanceCloudConnectorTest {

    private static final String FLEX_ID = "flex1";

    private final SimulatedCloud cloud = SimulatedClouds.basicCloud(1);

    private final SimulatedTitusAgentCluster FLEX_INSTANCE_GROUP = cloud.getAgentInstanceGroup(FLEX_ID);
    private final List<String> FLEX_INSTANCE_IDS = FLEX_INSTANCE_GROUP.getAgents().stream().map(SimulatedTitusAgent::getId).collect(Collectors.toList());

    private InstanceCloudConnector cloudConnector;

    protected abstract InstanceCloudConnector setup(SimulatedCloud cloud);

    @Before
    public void setUp() {
        this.cloudConnector = setup(cloud);
    }

    @Test
    public void testGetInstanceGroups() {
        List<InstanceGroup> found = cloudConnector.getInstanceGroups().toBlocking().first();
        assertThat(found).hasSize(3);
    }

    @Test
    public void testGetInstanceGroupByIds() {
        List<String> twoIds = cloudConnector.getInstanceGroups().toBlocking().first().subList(0, 2).stream().map(InstanceGroup::getId).collect(Collectors.toList());
        List<InstanceGroup> found = cloudConnector.getInstanceGroups(twoIds).toBlocking().first();
        assertThat(found.stream().map(InstanceGroup::getId)).isEqualTo(twoIds);
        assertThat(found.get(0).getInstanceIds()).hasSize(1);
    }

    @Test
    public void testGetInstanceLaunchConfiguration() {
        List<InstanceLaunchConfiguration> launchConfiguration = cloudConnector.getInstanceLaunchConfiguration(
                singletonList(FLEX_INSTANCE_GROUP.getName())
        ).toBlocking().first();
        assertThat(launchConfiguration.get(0).getInstanceType()).isEqualTo(FLEX_INSTANCE_GROUP.getInstanceType().getDescriptor().getId());
    }

    @Test
    public void testGetInstanceTypeResourceDimension() {
        AwsInstanceDescriptor awsInstanceDescriptor = FLEX_INSTANCE_GROUP.getInstanceType().getDescriptor();
        ResourceDimension resources = cloudConnector.getInstanceTypeResourceDimension(awsInstanceDescriptor.getId());
        assertThat(resources.getCpu()).isEqualTo(awsInstanceDescriptor.getvCPUs());
    }

    @Test
    public void testGetInstances() {
        List<Instance> instances = cloudConnector.getInstances(FLEX_INSTANCE_IDS).toBlocking().first();
        assertThat(instances.stream().map(Instance::getId)).containsExactlyElementsOf(FLEX_INSTANCE_IDS);
    }

    @Test
    public void testGetInstancesByInstanceGroupId() {
        List<Instance> instances = cloudConnector.getInstancesByInstanceGroupId(FLEX_INSTANCE_GROUP.getName()).toBlocking().first();
        assertThat(instances.stream().map(Instance::getId)).containsExactlyElementsOf(FLEX_INSTANCE_IDS);
    }

    @Test
    public void testUpdateCapacity() {
        Throwable error = cloudConnector.updateCapacity(FLEX_INSTANCE_GROUP.getName(), Optional.of(1), Optional.of(2)).get();
        assertThat(error).isNull();

        InstanceGroup updatedFlex = cloudConnector.getInstanceGroups(singletonList(FLEX_ID)).toBlocking().first().get(0);
        assertThat(updatedFlex.getMin()).isEqualTo(1);
        assertThat(updatedFlex.getDesired()).isEqualTo(2);
    }

    @Test
    public void testScaleUp() {
        Throwable error = cloudConnector.scaleUp(FLEX_INSTANCE_GROUP.getName(), 1).get();
        assertThat(error).isNull();

        InstanceGroup updatedFlex = cloudConnector.getInstanceGroups(singletonList(FLEX_ID)).toBlocking().first().get(0);
        assertThat(updatedFlex.getDesired()).isEqualTo(2);
    }

    @Test
    public void testTerminateInstances() {
        List<Either<Boolean, Throwable>> result = cloudConnector.terminateInstances(FLEX_ID, FLEX_INSTANCE_IDS, false).toBlocking().first();
        assertThat(result.get(0).hasValue()).isTrue();
        assertThat(result.get(0).getValue()).isTrue();
    }
}
