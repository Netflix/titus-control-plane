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

package com.netflix.titus.runtime.connector.agent;

import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorAgentManagementServiceStub {

    Mono<AgentInstanceGroups> getInstanceGroups();

    Mono<AgentInstanceGroup> getInstanceGroup(Id id);

    Mono<AgentInstance> getAgentInstance(Id id);

    Mono<AgentInstances> findAgentInstances(AgentQuery query);

    Mono<Void> updateInstanceGroupTier(TierUpdate tierUpdate);

    Mono<Void> updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate);

    Mono<Void> updateInstanceGroupAttributes(InstanceGroupAttributesUpdate attributesUpdate);

    Mono<Void> updateAgentInstanceAttributes(AgentInstanceAttributesUpdate attributesUpdate);

    Flux<AgentChangeEvent> observeAgents();
}
