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

package io.netflix.titus.testkit.embedded.cloud.endpoint.representation;

import java.util.List;

import io.netflix.titus.common.aws.AwsInstanceType;

public class SimulatedAgentGroupRepresentation {

    private final String name;
    private final AwsInstanceType instanceType;
    private final int currentSize;
    private final List<SimulatedAgentRepresentation> agents;

    public SimulatedAgentGroupRepresentation(String name,
                                             AwsInstanceType instanceType,
                                             int currentSize,
                                             List<SimulatedAgentRepresentation> agents) {

        this.name = name;
        this.instanceType = instanceType;
        this.currentSize = currentSize;
        this.agents = agents;
    }

    public String getName() {
        return name;
    }

    public AwsInstanceType getInstanceType() {
        return instanceType;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public List<SimulatedAgentRepresentation> getAgents() {
        return agents;
    }
}
