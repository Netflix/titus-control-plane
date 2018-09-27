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

package com.netflix.titus.testkit.model.agent;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.master.model.ResourceDimensions;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.common.data.generator.DataGenerator.rangeInt;
import static com.netflix.titus.testkit.model.PrimitiveValueGenerators.ipv4CIDRs;
import static java.util.Arrays.asList;

public final class AgentGenerator {

    public static final String ATTR_SUBNET = "subnet";

    public static final int MAX_SERVER_GROUP_SIZE = 100;
    public static final int MAX_IDLE_TO_KEEP = 20;

    private AgentGenerator() {
    }

    public static DataGenerator<String> instanceTypes() {
        return DataGenerator.items("m4.xlarge", "m4.2xlarge", "m4.4xlarge", "m4.10xlarge");
    }

    public static DataGenerator<Tier> tiers() {
        return DataGenerator.items(asList(Tier.values()));
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups(Tier tier, int desiredSize) {
        return agentServerGroups(tier, desiredSize, instanceTypes().toList());
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups(Tier tier, int desiredSize, AwsInstanceType instanceType) {
        AwsInstanceDescriptor awsInstanceDescriptor = instanceType.getDescriptor();
        return agentServerGroups(tier, desiredSize, Collections.singletonList(instanceType.name()))
                .map(instanceGroup ->
                        instanceGroup.toBuilder()
                                .withResourceDimension(
                                        ResourceDimension.newBuilder()
                                                .withCpus(awsInstanceDescriptor.getvGPUs())
                                                .withGpu(awsInstanceDescriptor.getvGPUs())
                                                .withMemoryMB(awsInstanceDescriptor.getMemoryGB() * 1024)
                                                .withDiskMB(awsInstanceDescriptor.getStorageGB() * 1024)
                                                .withNetworkMbs(awsInstanceDescriptor.getNetworkMbs())
                                                .build()
                                )
                                .build());
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups(Tier tier, int desiredSize, List<String> instanceTypes) {
        return DataGenerator.bindBuilder(AgentInstanceGroup::newBuilder)
                .bind(range(0), (builder, idx) -> {
                    builder.withId("AgentInstanceGroup#" + idx);
                    builder.withAttributes(Collections.singletonMap(ATTR_SUBNET, idx + ".0.0.0/8"));
                })
                .bind(DataGenerator.items(instanceTypes), (builder, instanceType) -> builder
                        .withInstanceType(instanceType)
                        .withResourceDimension(ResourceDimensions.fromAwsInstanceType(AwsInstanceType.withName(instanceType)))
                )
                .map(builder -> builder
                        .withTier(tier)
                        .withMin(0)
                        .withDesired(desiredSize)
                        .withCurrent(desiredSize)
                        .withMax(desiredSize)
                        .withLaunchTimestamp(System.currentTimeMillis())
                        .withTimestamp(System.currentTimeMillis())
                        .withIsLaunchEnabled(true)
                        .withIsTerminateEnabled(true)
                        .withAttributes(Collections.emptyMap())
                        .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                                .withState(InstanceGroupLifecycleState.Active)
                                .withDetail("ASG activated")
                                .withTimestamp(System.currentTimeMillis())
                                .build()
                        )
                )
                .map(AgentInstanceGroup.Builder::build);
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups() {
        return DataGenerator.zip(
                agentServerGroups(Tier.Flex, 1),
                rangeInt(1, 10)
        ).map(pair -> {
                    AgentInstanceGroup serverGroup = pair.getLeft();
                    int desired = pair.getRight();
                    return serverGroup.toBuilder()
                            .withDesired(desired)
                            .withMax(desired)
                            .withTier(Tier.values()[desired % Tier.values().length])
                            .build();
                }
        );
    }

    public static DataGenerator<AgentInstance> agentInstances(AgentInstanceGroup serverGroup) {
        AgentInstance.Builder template = AgentInstance.newBuilder()
                .withInstanceGroupId(serverGroup.getId())
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder()
                        .withState(InstanceLifecycleState.Started)
                        .withLaunchTimestamp(System.currentTimeMillis())
                        .build()
                )
                .withAttributes(Collections.emptyMap());
        String subnet = serverGroup.getAttributes().getOrDefault(ATTR_SUBNET, "10.0.0.0/8");

        return DataGenerator.bindBuilder(template::but)
                .bind(range(0).map(idx -> serverGroup.getId() + '#' + idx), AgentInstance.Builder::withId)
                .bind(ipv4CIDRs(subnet), (builder, ip) -> {
                    builder.withIpAddress(ip);
                    builder.withHostname(ip.replace('.', '_') + ".titus.netflix.dev");
                })
                .map(builder -> builder
                        .withTimestamp(System.currentTimeMillis())
                        .build()
                );
    }

    public static DataGenerator<AgentInstance> agentInstances() {
        return agentServerGroups().flatMap(serverGroup -> agentInstances(serverGroup).limit(10));
    }
}