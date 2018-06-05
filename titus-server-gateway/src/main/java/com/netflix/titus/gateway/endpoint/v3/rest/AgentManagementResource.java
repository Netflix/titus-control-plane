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

package com.netflix.titus.gateway.endpoint.v3.rest;

import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Strings;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.gateway.endpoint.v3.rest.representation.TierWrapper;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import static com.netflix.titus.runtime.endpoint.v3.rest.RestUtil.createPage;
import static com.netflix.titus.runtime.endpoint.v3.rest.RestUtil.getFieldsParameter;
import static com.netflix.titus.runtime.endpoint.v3.rest.RestUtil.getFilteringCriteria;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Agent Management")
@Path("/v3/agent")
@Singleton
public class AgentManagementResource {

    private final AgentManagementClient agentManagementService;

    @Inject
    public AgentManagementResource(AgentManagementClient agentManagementService) {
        this.agentManagementService = agentManagementService;
    }

    @GET
    @ApiOperation("Get all agent instance groups")
    @Path("/instanceGroups")
    public AgentInstanceGroups getInstanceGroups() {
        return Responses.fromSingleValueObservable(agentManagementService.getInstanceGroups());
    }

    @GET
    @ApiOperation("Get an agent instance group with the given id")
    @Path("/instanceGroups/{id}")
    public AgentInstanceGroup getInstanceGroup(@PathParam("id") String id) {
        return Responses.fromSingleValueObservable(agentManagementService.getInstanceGroup(id));
    }

    @GET
    @ApiOperation("Get an agent instance with the given id")
    @Path("/instances/{id}")
    public AgentInstance getAgentInstance(String id) {
        return Responses.fromSingleValueObservable(agentManagementService.getAgentInstance(id));
    }

    @GET
    @ApiOperation("Find agent instances")
    @Path("/instances")
    public AgentInstances findAgentInstances(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        AgentQuery.Builder queryBuilder = AgentQuery.newBuilder();
        queryBuilder.setPage(createPage(queryParameters));
        queryBuilder.putAllFilteringCriteria(getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(agentManagementService.findAgentInstances(queryBuilder.build()));
    }

    @PUT
    @ApiOperation("Update instance group tier")
    @Path("/instanceGroups/{id}/tier")
    public Response updateInstanceGroupTier(@PathParam("id") String instanceGroupId, TierWrapper tierWrapper) {
        TierUpdate tierUpdate = TierUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .setTier(GrpcAgentModelConverters.toGrpcTier(tierWrapper.getTier()))
                .build();
        return Responses.fromCompletable(agentManagementService.updateInstanceGroupTier(tierUpdate));
    }

    @PUT
    @ApiOperation("Update auto scaling rule configuration")
    @Path("/instanceGroups/{id}/autoscale")
    public Response updateAutoScalingRule(AutoScalingRuleUpdate autoScalingRuleUpdate) {
        return Responses.fromCompletable(agentManagementService.updateAutoScalingRule(autoScalingRuleUpdate));
    }

    @PUT
    @ApiOperation("Update instance group lifecycle configuration")
    @Path("/instanceGroups/{id}/lifecycle")
    public Response updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return Responses.fromCompletable(agentManagementService.updateInstanceGroupLifecycle(lifecycleStateUpdate));
    }

    @PUT
    @ApiOperation("Update instance group attributes")
    @Path("/instanceGroups/{id}/attributes")
    public Response updateInstanceGroupLifecycle(@PathParam("id") String instanceGroupId, InstanceGroupAttributesUpdate attributesUpdate) {
        if (Strings.isNullOrEmpty(attributesUpdate.getInstanceGroupId())) {
            attributesUpdate = attributesUpdate.toBuilder().setInstanceGroupId(instanceGroupId).build();
        } else if (!Objects.equals(instanceGroupId, attributesUpdate.getInstanceGroupId())) {
            throw TitusServiceException.invalidArgument("Path parameter id: " + instanceGroupId + " must match payload instanceGroupId: "
                    + attributesUpdate.getInstanceGroupId());
        }

        return Responses.fromCompletable(agentManagementService.updateInstanceGroupAttributes(attributesUpdate));
    }

    @PUT
    @ApiOperation("Update agent instance override status")
    @Path("/instances/{id}/override")
    public Response updateInstanceOverride(InstanceOverrideStateUpdate overrideStateUpdate) {
        return Responses.fromCompletable(agentManagementService.updateInstanceOverride(overrideStateUpdate));
    }
}
