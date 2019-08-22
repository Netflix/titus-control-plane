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

package com.netflix.titus.master.integration.v3.scenario;

import java.time.Instant;
import java.util.function.Consumer;

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;

public class InstanceGroupScenarioTemplates {

    /**
     * Instance groups activation template for {@link SimulatedClouds#basicCloud(int)} setup.
     */
    public static Consumer<InstanceGroupsScenarioBuilder> basicCloudActivation() {
        return scenarioBuilder -> scenarioBuilder
                .apply("critical1", g -> g.tier(Tier.Critical).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flex1", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flexGpu", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active));
    }

    /**
     * Instance groups activation template for {@link SimulatedClouds#twoPartitionsPerTierStack(int)} setup.
     */
    public static Consumer<InstanceGroupsScenarioBuilder> twoPartitionsPerTierStackActivation() {
        return scenarioBuilder -> scenarioBuilder
                .apply("critical1", g -> g.tier(Tier.Critical).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("critical2", g -> g.tier(Tier.Critical).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flex1", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flex2", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active));
    }

    public static Consumer<InstanceGroupsScenarioBuilder> activate(String... instanceGroups) {
        return updateLifecycleState(InstanceGroupLifecycleState.Active, instanceGroups);
    }

    public static Consumer<InstanceGroupsScenarioBuilder> deactivate(String... instanceGroups) {
        return updateLifecycleState(InstanceGroupLifecycleState.Inactive, instanceGroups);
    }

    public static Consumer<InstanceGroupsScenarioBuilder> evacuate(String... instanceGroups) {
        return updateLifecycleState(InstanceGroupLifecycleState.Removable, instanceGroups);
    }

    private static Consumer<InstanceGroupsScenarioBuilder> updateLifecycleState(InstanceGroupLifecycleState state, String[] instanceGroups) {
        return scenarioBuilder -> {
            for (String instanceGroup : instanceGroups) {
                scenarioBuilder.apply(instanceGroup, b -> {
                    b.lifecycleState(state);
                });
            }
        };
    }
}
