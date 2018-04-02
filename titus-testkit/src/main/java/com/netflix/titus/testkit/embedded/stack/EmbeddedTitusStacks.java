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

import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters;

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
