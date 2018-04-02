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

package com.netflix.titus.testkit.stub.connector.cloud;

import java.util.Collections;

import com.netflix.titus.testkit.model.PrimitiveValueGenerators;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.common.data.generator.DataGenerator;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.testkit.model.PrimitiveValueGenerators.ipv4CIDRs;

/**
 * Test data generator.
 */
public final class InstanceGenerators {

    public static final String ATTR_SUBNET = "subnet";

    public static DataGenerator<InstanceGroup> instanceGroups(int desiredSize) {
        return range(1).map(idx -> InstanceGroup.newBuilder()
                .withId("instanceGroup-" + idx)
                .withLaunchConfigurationName("launchConfiguration-" + idx)
                .withMin(0)
                .withDesired(desiredSize)
                .withMax(desiredSize)
                .withAttributes(Collections.singletonMap(ATTR_SUBNET, idx + ".0.0.0/8"))
                .withInstanceIds(Collections.emptyList())
                .build()
        );
    }

    public static DataGenerator<Instance> instances(InstanceGroup instanceGroup) {
        Instance.Builder template = Instance.newBuilder()
                .withInstanceGroupId(instanceGroup.getId())
                .withInstanceState(Instance.InstanceState.Running)
                .withAttributes(Collections.emptyMap());
        String subnet = instanceGroup.getAttributes().getOrDefault(ATTR_SUBNET, "10.0.0.0/8");

        return DataGenerator.bindBuilder(template::but)
                .bind(range(1).map(idx -> instanceGroup.getId() + '#' + idx), Instance.Builder::withId)
                .bind(PrimitiveValueGenerators.ipv4CIDRs(subnet), (builder, ip) -> {
                    builder.withIpAddress(ip);
                    builder.withHostname(ip.replace('.', '_') + ".titus.netflix.dev");
                })
                .map(Instance.Builder::build);
    }
}
