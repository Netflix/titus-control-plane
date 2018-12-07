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

package com.netflix.titus.testkit.embedded.cloud.connector.remote;

import com.netflix.titus.simulator.TitusCloudSimulator.AddInstanceGroupRequest;
import com.netflix.titus.simulator.TitusCloudSimulator.CapacityUpdateRequest;
import com.netflix.titus.simulator.TitusCloudSimulator.Id;
import com.netflix.titus.simulator.TitusCloudSimulator.Ids;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface SimulatedAgentClient {

    Mono<Void> addInstanceGroup(AddInstanceGroupRequest request);

    Flux<SimulatedInstanceGroup> getAllInstanceGroups();

    Flux<SimulatedInstanceGroup> getInstanceGroups(Ids ids);

    Mono<SimulatedInstance> getInstancesOfInstanceGroup(Id id);

    Flux<SimulatedInstance> getInstances(Ids ids);

    Mono<Void> updateCapacity(CapacityUpdateRequest capacityUpdateRequest);

    Mono<Void> removeInstanceGroup(Id id);

    Mono<Void> terminateInstance(Id id);

    Mono<Void> terminateAndShrinkInstance(Id id);
}
