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

package com.netflix.titus.testkit.embedded.cloud.endpoint.rest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTask;
import com.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudGateway;
import com.netflix.titus.testkit.embedded.cloud.endpoint.rest.representation.AddSimulatedInstanceGroup;
import com.sun.jersey.spi.resource.Singleton;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudGateway;
import com.netflix.titus.testkit.embedded.cloud.endpoint.rest.representation.AddSimulatedInstanceGroup;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/agents")
@Singleton
public class SimulatedAgentsServiceResource {

    private final SimulatedCloudGateway gateway;

    @Inject
    public SimulatedAgentsServiceResource(SimulatedCloudGateway gateway) {
        this.gateway = gateway;
    }

    @GET
    @Path("/instanceGroups")
    public List<SimulatedInstanceGroup> getAllInstanceGroups(@QueryParam("ids") List<String> ids) {
        if (ids.isEmpty()) {
            return gateway.getAllInstanceGroups();
        }
        return gateway.getInstanceGroups(new HashSet<>(ids));
    }

    @POST
    @Path("/instanceGroups")
    public Response addInstanceGroup(AddSimulatedInstanceGroup newInstanceGroup) {
        gateway.addInstanceGroup(
                newInstanceGroup.getId(),
                AwsInstanceType.withName(newInstanceGroup.getInstanceType()),
                newInstanceGroup.getMin(),
                newInstanceGroup.getDesired(),
                newInstanceGroup.getMax()
        );
        return Response.noContent().build();
    }

    @GET
    @Path("/instances")
    public List<SimulatedInstance> getAllInstances(@QueryParam("ids") List<String> ids) {
        List<SimulatedInstance> allInstances = gateway.getAllInstanceGroups().stream().flatMap(g -> gateway.getInstances(g.getId()).stream()).collect(Collectors.toList());
        if (ids.isEmpty()) {
            return allInstances;
        }
        Set<String> idSet = new HashSet<>(ids);
        return allInstances.stream().filter(i -> idSet.contains(i.getId())).collect(Collectors.toList());
    }

    @GET
    @Path("/instanceGroups/{instanceGroupId}/instances")
    public List<SimulatedInstance> getInstances(@PathParam("instanceGroupId") String instanceGroupId) {
        return gateway.getInstances(instanceGroupId);
    }

    @GET
    @Path("/tasks")
    public List<SimulatedTask> getSimulatedTasks(@QueryParam("ids") List<String> ids) {
        return gateway.getSimulatedTasks(new HashSet<>(ids));
    }

    @GET
    @Path("/instances/{instanceId}/tasks")
    public List<SimulatedTask> getSimulatedTasks(@PathParam("instanceId") String instanceId) {
        return gateway.getSimulatedTasksOnInstance(instanceId);
    }

    @DELETE
    @Path("/tasks")
    public Response getSimulatedTasks() {
        gateway.terminateAllTasks();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/tasks/{taskId}")
    public Response terminateTask(@PathParam("taskId") String taskId) {
        boolean result = gateway.terminateTask(taskId);
        return result ? Response.noContent().build() : Response.status(Response.Status.NOT_FOUND).build();
    }

    @PUT
    @Path("/instanceGroups/{instanceGroupId}/capacity")
    public Response updateCapacity(@PathParam("instanceGroupId") String instanceGroupId,
                                   SimulatedInstanceGroup.Capacity capacity) {
        gateway.updateCapacity(instanceGroupId, capacity);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/instances/{instanceId}")
    public Response terminateInstance(@PathParam("instanceId") String instanceId,
                                      @QueryParam("shrink") boolean shrink) {
        gateway.terminateInstance(instanceId, shrink);
        return Response.noContent().build();
    }
}
