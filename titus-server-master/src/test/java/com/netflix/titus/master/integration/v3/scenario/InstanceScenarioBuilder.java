/*
 * Copyright 2019 Netflix, Inc.
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

import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.testkit.junit.master.TitusStackResource;

public class InstanceScenarioBuilder {
    private final TitusStackResource titusStackResource;
    private final AgentInstance instance;

    InstanceScenarioBuilder(TitusStackResource titusStackResource, AgentInstance instance) {
        this.titusStackResource = titusStackResource;
        this.instance = instance;
    }

    public InstanceScenarioBuilder addOpportunisticCpus(OpportunisticCpuAvailability availability) {
        titusStackResource.getMaster().addOpportunisticCpu(instance.getId(), availability);
        return this;
    }
}
