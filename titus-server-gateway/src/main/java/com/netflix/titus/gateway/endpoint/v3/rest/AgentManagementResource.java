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

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.gateway.endpoint.v3.rest.representation.TierWrapper;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceAttributesUpdate;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Agent Management")
@Path("/v3/agent")
@Singleton
/**
 * @deprecated Remove this stub after confirming that no clients depend on this API.
 */
public class AgentManagementResource {

    @Inject
    public AgentManagementResource() {
    }

    @GET
    @ApiOperation("Get all agent instance groups")
    @Path("/instanceGroups")
    public AgentInstanceGroups getInstanceGroups() {
        return AgentInstanceGroups.getDefaultInstance();
    }

    @GET
    @ApiOperation("Get an agent instance group with the given id")
    @Path("/instanceGroups/{id}")
    public AgentInstanceGroup getInstanceGroup(@PathParam("id") String id) {
        throw AgentManagementException.agentGroupNotFound(id);
    }

    @GET
    @ApiOperation("Get an agent instance with the given id")
    @Path("/instances/{id}")
    public AgentInstance getAgentInstance(@PathParam("id") String id) {
        throw AgentManagementException.agentNotFound(id);
    }

    @GET
    @ApiOperation("Find agent instances")
    @Path("/instances")
    public AgentInstances findAgentInstances(@Context UriInfo info) {
        return AgentInstances.getDefaultInstance();
    }

    @PUT
    @ApiOperation("Update instance group tier")
    @Path("/instanceGroups/{id}/tier")
    public Response updateInstanceGroupTier(@PathParam("id") String instanceGroupId, TierWrapper tierWrapper) {
        throw AgentManagementException.agentGroupNotFound(instanceGroupId);
    }

    @PUT
    @ApiOperation("Update instance group lifecycle configuration")
    @Path("/instanceGroups/{id}/lifecycle")
    public Response updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        throw AgentManagementException.agentGroupNotFound(lifecycleStateUpdate.getInstanceGroupId());
    }

    @PUT
    @ApiOperation("Update instance group attributes")
    @Path("/instanceGroups/{id}/attributes")
    public Response updateInstanceGroupAttributes(@PathParam("id") String instanceGroupId, InstanceGroupAttributesUpdate attributesUpdate) {
        throw AgentManagementException.agentGroupNotFound(instanceGroupId);
    }

    @DELETE
    @ApiOperation("Delete instance group attributes")
    @Path("/instanceGroups/{id}/attributes")
    public Response deleteInstanceGroupAttributes(@PathParam("id") String instanceGroupId, @QueryParam("keys") String delimitedKeys) {
        throw AgentManagementException.agentGroupNotFound(instanceGroupId);
    }

    @PUT
    @ApiOperation("Update agent instance attributes")
    @Path("/instances/{id}/attributes")
    public Response updateAgentInstanceAttributes(@PathParam("id") String agentInstanceId, AgentInstanceAttributesUpdate attributesUpdate) {
        throw AgentManagementException.agentNotFound(agentInstanceId);
    }

    @DELETE
    @ApiOperation("Delete agent instance attributes")
    @Path("/instances/{id}/attributes")
    public Response deleteAgentInstanceAttributes(@PathParam("id") String agentInstanceId, @QueryParam("keys") String delimitedKeys) {
        throw AgentManagementException.agentNotFound(agentInstanceId);
    }
}
