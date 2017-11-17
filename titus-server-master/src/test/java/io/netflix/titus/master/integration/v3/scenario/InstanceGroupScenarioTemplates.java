package io.netflix.titus.master.integration.v3.scenario;

import java.util.function.Consumer;

import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.testkit.embedded.cloud.SimulatedClouds;

public class InstanceGroupScenarioTemplates {

    /**
     * Instance groups activation template for {@link SimulatedClouds#basicCloud(int)} setup.
     */
    public static Consumer<InstanceGroupsScenarioBuilder> basicSetupActivation() {
        return scenarioBuilder -> scenarioBuilder
                .apply("critical1", g -> g.tier(Tier.Critical).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flex1", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active))
                .apply("flexGpu", g -> g.tier(Tier.Flex).lifecycleState(InstanceGroupLifecycleState.Active));
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
