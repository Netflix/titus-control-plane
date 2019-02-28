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
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.gateway.endpoint.v3.rest.representation.TierWrapper;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.DeleteAgentInstanceAttributesRequest;
import com.netflix.titus.grpc.protogen.DeleteInstanceGroupAttributesRequest;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.runtime.connector.agent.ReactorAgentManagementServiceStub;
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

    private final ReactorAgentManagementServiceStub agentManagementService;

    @Inject
    public AgentManagementResource(ReactorAgentManagementServiceStub agentManagementService) {
        this.agentManagementService = agentManagementService;
    }

    @GET
    @ApiOperation("Get all agent instance groups")
    @Path("/instanceGroups")
    public AgentInstanceGroups getInstanceGroups() {
        return Responses.fromMono(agentManagementService.getInstanceGroups());
    }

    @GET
    @ApiOperation("Get an agent instance group with the given id")
    @Path("/instanceGroups/{id}")
    public AgentInstanceGroup getInstanceGroup(@PathParam("id") String id) {
        return Responses.fromMono(agentManagementService.getInstanceGroup(Id.newBuilder().setId(id).build()));
    }

    @GET
    @ApiOperation("Get an agent instance with the given id")
    @Path("/instances/{id}")
    public AgentInstance getAgentInstance(@PathParam("id") String id) {
        return Responses.fromMono(agentManagementService.getAgentInstance(Id.newBuilder().setId(id).build()));
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
        return Responses.fromMono(agentManagementService.findAgentInstances(queryBuilder.build()));
    }

    @PUT
    @ApiOperation("Update instance group tier")
    @Path("/instanceGroups/{id}/tier")
    public Response updateInstanceGroupTier(@PathParam("id") String instanceGroupId, TierWrapper tierWrapper) {
        TierUpdate tierUpdate = TierUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .setTier(GrpcAgentModelConverters.toGrpcTier(tierWrapper.getTier()))
                .build();
        return Responses.fromVoidMono(agentManagementService.updateInstanceGroupTier(tierUpdate));
    }

    @PUT
    @ApiOperation("Update instance group lifecycle configuration")
    @Path("/instanceGroups/{id}/lifecycle")
    public Response updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return Responses.fromVoidMono(agentManagementService.updateInstanceGroupLifecycleState(lifecycleStateUpdate));
    }

    @PUT
    @ApiOperation("Update instance group attributes")
    @Path("/instanceGroups/{id}/attributes")
    public Response updateInstanceGroupAttributes(@PathParam("id") String instanceGroupId, InstanceGroupAttributesUpdate attributesUpdate) {
        if (Strings.isNullOrEmpty(attributesUpdate.getInstanceGroupId())) {
            attributesUpdate = attributesUpdate.toBuilder().setInstanceGroupId(instanceGroupId).build();
        } else if (!Objects.equals(instanceGroupId, attributesUpdate.getInstanceGroupId())) {
            throw TitusServiceException.invalidArgument("Path parameter id: " + instanceGroupId + " must match payload instanceGroupId: "
                    + attributesUpdate.getInstanceGroupId());
        }

        return Responses.fromVoidMono(agentManagementService.updateInstanceGroupAttributes(attributesUpdate));
    }

    @DELETE
    @ApiOperation("Delete instance group attributes")
    @Path("/instanceGroups/{id}/attributes")
    public Response deleteInstanceGroupAttributes(@PathParam("id") String instanceGroupId, @PathParam("keys") String delimitedKeys) {
        if (Strings.isNullOrEmpty(delimitedKeys)) {
            throw TitusServiceException.invalidArgument("Path parameter keys cannot be empty");
        }

        Set<String> keys = StringExt.splitByCommaIntoSet(delimitedKeys);
        if (keys.isEmpty()) {
            throw TitusServiceException.invalidArgument("Parsed path parameter keys cannot be empty");
        }

        DeleteInstanceGroupAttributesRequest request = DeleteInstanceGroupAttributesRequest.newBuilder()
                .setInstanceGroupId(instanceGroupId)
                .addAllKeys(keys)
                .build();
        return Responses.fromVoidMono(agentManagementService.deleteInstanceGroupAttributes(request));
    }

    @PUT
    @ApiOperation("Update agent instance attributes")
    @Path("/instances/{id}/attributes")
    public Response updateAgentInstanceAttributes(@PathParam("id") String agentInstanceId, AgentInstanceAttributesUpdate attributesUpdate) {
        if (Strings.isNullOrEmpty(attributesUpdate.getAgentInstanceId())) {
            attributesUpdate = attributesUpdate.toBuilder().setAgentInstanceId(agentInstanceId).build();
        } else if (!Objects.equals(agentInstanceId, attributesUpdate.getAgentInstanceId())) {
            throw TitusServiceException.invalidArgument("Path parameter id: " + agentInstanceId + " must match payload agentInstanceId: "
                    + attributesUpdate.getAgentInstanceId());
        }

        return Responses.fromVoidMono(agentManagementService.updateAgentInstanceAttributes(attributesUpdate));
    }

    @DELETE
    @ApiOperation("Delete agent instance attributes")
    @Path("/instances/{id}/attributes")
    public Response deleteAgentInstanceAttributes(@PathParam("id") String agentInstanceId, @PathParam("keys") String delimitedKeys) {
        if (Strings.isNullOrEmpty(delimitedKeys)) {
            throw TitusServiceException.invalidArgument("Path parameter keys cannot be empty");
        }

        Set<String> keys = StringExt.splitByCommaIntoSet(delimitedKeys);
        if (keys.isEmpty()) {
            throw TitusServiceException.invalidArgument("Parsed path parameter keys cannot be empty");
        }

        DeleteAgentInstanceAttributesRequest request = DeleteAgentInstanceAttributesRequest.newBuilder()
                .setAgentInstanceId(agentInstanceId)
                .addAllKeys(keys)
                .build();
        return Responses.fromVoidMono(agentManagementService.deleteAgentInstanceAttributes(request));
    }
}
