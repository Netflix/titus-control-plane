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

package com.netflix.titus.testkit.embedded.cloud.connector;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
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
