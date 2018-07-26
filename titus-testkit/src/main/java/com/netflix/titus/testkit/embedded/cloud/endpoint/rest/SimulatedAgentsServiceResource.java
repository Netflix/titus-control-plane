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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTask;
import com.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudGateway;
import com.netflix.titus.testkit.embedded.cloud.endpoint.rest.representation.AddSimulatedInstanceGroup;
import com.sun.jersey.spi.resource.Singleton;

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
    public List<SimulatedInstanceGroup> getInstanceGroups(@QueryParam("ids") List<String> ids) {
        if (ids.isEmpty()) {
            return gateway.getAllInstanceGroups();
        }
        return gateway.getInstanceGroups(new HashSet<>(ids));
    }

    @GET
    @Path("/instanceGroups/{id}")
    public SimulatedInstanceGroup getInstanceGroup(@PathParam("id") String id) {
        return getInstanceGroupOrThrowNotFound(id);
    }

    @GET
    @Path("/instanceGroups/{id}/allocations")
    public List<String> getInstanceGroupAllocations(@PathParam("id") String id) {
        SimulatedInstanceGroup instanceGroup = getInstanceGroupOrThrowNotFound(id);

        List<String> result = new ArrayList<>();
        for (SimulatedInstance instance : gateway.getInstances(id)) {
            List<SimulatedTask> tasks = gateway.getSimulatedTasksOnInstance(instance.getId());

            // We could improve this by summing dominant resources, but it is good enough to start.
            int usedCpus = tasks.stream().mapToInt(t -> t.getComputeResources().getCpu()).sum();
            int used = (64 * usedCpus) / instanceGroup.getComputeResources().getCpu();
            String usage = String.format("%s%s", toCharSeq(used, '#'), toCharSeq(64 - used, '_'));
            result.add(String.format("%30s: |%s|", instance.getId(), usage));
        }

        return result;
    }

    private String toCharSeq(int len, char c) {
        char[] cbuf = new char[len];
        Arrays.fill(cbuf, 0, len, c);
        return new String(cbuf);
    }

    private SimulatedInstanceGroup getInstanceGroupOrThrowNotFound(@QueryParam("ids") String id) {
        List<SimulatedInstanceGroup> instanceGroups = gateway.getInstanceGroups(Collections.singleton(id));
        if (instanceGroups.isEmpty()) {
            throw new WebApplicationException(Response.status(404).build());
        }
        return CollectionsExt.first(instanceGroups);
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
