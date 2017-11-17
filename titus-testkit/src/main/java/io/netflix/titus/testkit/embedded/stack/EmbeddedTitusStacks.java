package io.netflix.titus.testkit.embedded.stack;

import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import io.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters;

public class EmbeddedTitusStacks {

    public static EmbeddedTitusStack basicStack(int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.basicCloud(desired);
        return EmbeddedTitusStack.aTitusStack()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud))
                .withGateway(EmbeddedTitusGateway.aDefaultTitusGateway().build())
                .build();
    }

    public static EmbeddedTitusStack twoPartitionsPerTierStack(int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.twoPartitionsPerTierStack(desired);
        return EmbeddedTitusStack.aTitusStack()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud))
                .withGateway(EmbeddedTitusGateway.aDefaultTitusGateway().build())
                .build();
    }
}
