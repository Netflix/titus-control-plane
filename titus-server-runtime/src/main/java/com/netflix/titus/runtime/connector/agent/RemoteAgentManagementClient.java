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

package com.netflix.titus.runtime.connector.agent;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class RemoteAgentManagementClient implements AgentManagementClient {

    private final ReactorAgentManagementServiceStub stub;

    @Inject
    public RemoteAgentManagementClient(ReactorAgentManagementServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public Mono<List<AgentInstanceGroup>> getInstanceGroups() {
        return stub.getInstanceGroups()
                .map(groups -> groups.getAgentInstanceGroupsList().stream()
                        .map(GrpcAgentModelConverters::toCoreAgentInstanceGroup)
                        .collect(Collectors.toList())
                );
    }

    @Override
    public Mono<AgentInstanceGroup> getInstanceGroup(String id) {
        return stub.getInstanceGroup(Id.newBuilder().setId(id).build()).map(GrpcAgentModelConverters::toCoreAgentInstanceGroup);
    }

    @Override
    public Mono<AgentInstance> getAgentInstance(String id) {
        return stub.getAgentInstance(Id.newBuilder().setId(id).build()).map(GrpcAgentModelConverters::toCoreAgentInstance);
    }

    @Override
    public Mono<PageResult<AgentInstance>> findAgentInstances(Map<String, String> filteringCriteria, Page page) {
        return stub.findAgentInstances(AgentQuery.newBuilder()
                .setPage(CommonGrpcModelConverters.toGrpcPage(page))
                .putAllFilteringCriteria(filteringCriteria)
                .build()
        ).map(grpcResult -> PageResult.pageOf(
                grpcResult.getAgentInstancesList().stream().map(GrpcAgentModelConverters::toCoreAgentInstance).collect(Collectors.toList()),
                // Agent management GRPC API does not support pagination yet, so we set some reasonable default value here.
                Pagination.newBuilder()
                        .withCurrentPage(page)
                        .withTotalItems(grpcResult.getAgentInstancesCount())
                        .withTotalPages(1)
                        .withCursor("")
                        .withHasMore(false)
                        .build()
        ));
    }

    @Override
    public Mono<Void> updateInstanceGroupTier(String instanceGroupId, Tier tier) {
        return stub.updateInstanceGroupTier(TierUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .setTier(GrpcAgentModelConverters.toGrpcTier(tier))
                .build()
        );
    }

    @Override
    public Mono<Void> updateInstanceGroupLifecycleState(String instanceGroupId, InstanceGroupLifecycleState lifecycleState) {
        return stub.updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .setLifecycleState(GrpcAgentModelConverters.toGrpcLifecycleState(lifecycleState))
                .build()
        );
    }

    @Override
    public Mono<Void> updateInstanceGroupAttributes(String instanceGroupId, Map<String, String> attributes) {
        return stub.updateInstanceGroupAttributes(InstanceGroupAttributesUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .putAllAttributes(attributes)
                .build()
        );
    }

    @Override
    public Mono<Void> updateAgentInstanceAttributes(String instanceId, Map<String, String> attributes) {
        return stub.updateAgentInstanceAttributes(AgentInstanceAttributesUpdate.newBuilder()
                .setAgentInstanceId(instanceId)
                .putAllAttributes(attributes)
                .build()
        );
    }

    @Override
    public Flux<AgentEvent> observeAgents() {
        return stub.observeAgents()
                .flatMap(grpcEvent -> GrpcAgentModelConverters.toCoreEvent(grpcEvent)
                        .map(Flux::just)
                        .orElse(Flux.empty())
                );
    }
}
