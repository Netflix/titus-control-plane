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

package io.netflix.titus.testkit.model.agent;

import java.util.Collections;
import java.util.List;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceLifecycleState;
import io.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceOverrideStatus;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.util.tuple.Pair;

import static io.netflix.titus.common.data.generator.DataGenerator.range;
import static io.netflix.titus.common.data.generator.DataGenerator.rangeInt;
import static io.netflix.titus.testkit.model.PrimitiveValueGenerators.ipv4CIDRs;
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

    public static DataGenerator<AutoScaleRule> autoScaleRules() {
        DataGenerator<Pair<Long, Long>> minMax = range(0, MAX_SERVER_GROUP_SIZE).map(min -> Pair.of(min, Math.min(MAX_SERVER_GROUP_SIZE, 5 + 2 * min)));
        DataGenerator<Pair<Long, Long>> minMaxIdleToKeep = range(0, MAX_IDLE_TO_KEEP).map(min -> Pair.of(min, Math.min(MAX_IDLE_TO_KEEP, 5 + 2 * min)));

        return DataGenerator.bindBuilder(AutoScaleRule::newBuilder)
                .bind(range(0).map(idx -> "AgentInstanceGroup#" + idx), AutoScaleRule.Builder::withInstanceGroupId)
                .bind(minMax, (builder, minMaxV) -> {
                    builder.withMin(Math.toIntExact(minMaxV.getLeft()));
                    builder.withMax(Math.toIntExact(minMaxV.getRight()));
                })
                .bind(minMaxIdleToKeep, (builder, minMaxV) -> {
                    builder.withMaxIdleToKeep(Math.toIntExact(minMaxV.getLeft()));
                    builder.withMaxIdleToKeep(Math.toIntExact(minMaxV.getRight()));
                })
                .bind(range(60, 600), (builder, cd) -> builder.withCoolDownSec(Math.toIntExact(cd)))
                .bind(range(0, 10), (builder, priority) -> builder.withPriority(Math.toIntExact(priority)))
                .map(AutoScaleRule.Builder::build);
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups(Tier tier, int desiredSize) {
        return agentServerGroups(tier, desiredSize, instanceTypes().toList());
    }

    public static DataGenerator<AgentInstanceGroup> agentServerGroups(Tier tier, int desiredSize, List<String> instanceTypes) {
        return DataGenerator.bindBuilder(AgentInstanceGroup::newBuilder)
                .bind(range(0), (builder, idx) -> {
                    builder.withId("AgentInstanceGroup#" + idx);
                    builder.withAttributes(Collections.singletonMap(ATTR_SUBNET, idx + ".0.0.0/8"));
                })
                .bind(DataGenerator.items(instanceTypes), AgentInstanceGroup.Builder::withInstanceType)
                .bind(autoScaleRules(), AgentInstanceGroup.Builder::withAutoScaleRule)
                .map(builder -> builder
                        .withTier(tier)
                        .withMin(0)
                        .withDesired(desiredSize)
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
                                .build())
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
                .withOverrideStatus(InstanceOverrideStatus.none())
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