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

package com.netflix.titus.testkit.embedded.stack;

import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters;

public class EmbeddedTitusStacks {

    public static EmbeddedTitusStack basicStack(int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.basicCloud(desired);

        EmbeddedTitusStack.Builder builder = EmbeddedTitusStack.aTitusStack()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud))
                .withDefaultGateway();

        return addFederation(builder).build();
    }

    public static EmbeddedTitusStack twoPartitionsPerTierStack(int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.twoPartitionsPerTierStack(desired);

        EmbeddedTitusStack.Builder builder = EmbeddedTitusStack.aTitusStack()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud))
                .withDefaultGateway();

        return addFederation(builder).build();
    }

    private static EmbeddedTitusStack.Builder addFederation(EmbeddedTitusStack.Builder builder) {
        boolean federationEnabled = "true".equalsIgnoreCase(System.getProperty("titus.test.federation", "true"));
        if (federationEnabled) {
            builder.withDefaultFederation();
        }
        return builder;
    }
}
