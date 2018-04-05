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

package com.netflix.titus.master.integration;

import com.netflix.titus.api.agent.model.AutoScaleRule;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor.awsInstanceGroup;
import static com.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters.basicMaster;
import static com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack.aTitusStack;

@Category(IntegrationTest.class)
public class AutoScalingTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(aTitusStack()
            .withMaster(basicMaster(new SimulatedCloud().createAgentInstanceGroups(
                    awsInstanceGroup("flex1", AwsInstanceType.M4_4XLarge, 0, 1, 10)
            )))
            .withDefaultGateway()
            .build()
    );

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.activate("flex1"));
    }

    @Test(timeout = 30_000)
    public void testAgentClustersAreScaledUpToMinIdleInstances() throws Exception {
        AutoScaleRule autoScaleRule = AutoScaleRule.newBuilder()
                .withMinIdleToKeep(5)
                .withMaxIdleToKeep(10)
                .withMin(0)
                .withMax(100)
                .withShortfallAdjustingFactor(1)
                .withCoolDownSec(1)
                .build();

        instanceGroupsScenarioBuilder.apply("flex1", b -> b
                .autoScaleRule(autoScaleRule)
                .awaitDesiredSize(10)
        );
    }
}
