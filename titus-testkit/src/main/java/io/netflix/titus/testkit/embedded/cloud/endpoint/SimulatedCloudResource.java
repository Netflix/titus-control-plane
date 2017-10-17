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

package io.netflix.titus.testkit.embedded.cloud.endpoint;

import java.util.Collections;
import java.util.List;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.spi.resource.Singleton;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.endpoint.representation.CapacityRepresentation;
import io.netflix.titus.testkit.embedded.cloud.endpoint.representation.SimulatedAgentGroupRepresentation;
import io.netflix.titus.testkit.embedded.cloud.endpoint.representation.SimulatedAgentRepresentation;
import io.netflix.titus.testkit.embedded.cloud.endpoint.representation.SimulatedContainerRepresentation;
import io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import org.apache.mesos.Protos;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/")
@Singleton
public class SimulatedCloudResource {

    private final SimulatedCloud simulatedCloud;

    @Inject
    public SimulatedCloudResource(SimulatedCloud simulatedCloud) {
        this.simulatedCloud = simulatedCloud;
    }

    @POST
    @Path("/agents/instanceGroups")
    public Response createAgentInstanceGroups(SimulatedAgentGroupDescriptor newAgentGroup) {
        simulatedCloud.createAgentInstanceGroups(newAgentGroup);
        return Response.noContent().build();
    }

    @GET
    @Path("/agents/instanceGroups")
    public List<SimulatedAgentGroupRepresentation> getAgentInstanceGroups() {
        return simulatedCloud.getAgentInstanceGroups().stream()
                .map(this::toAgentGroupRepresentation)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/agents/instanceGroups/{id}")
    public Response getAgentInstanceGroup(@PathParam("id") String instanceGroupId) {
        try {
            return Response.ok().entity(toAgentGroupRepresentation(simulatedCloud.getAgentInstanceGroup(instanceGroupId))).build();
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @GET
    @Path("/agents/instances")
    public List<SimulatedAgentRepresentation> getAgentInstances() {
        return simulatedCloud.getAgentInstanceGroups().stream()
                .flatMap(g -> g.getAgents().stream()).map(agent -> toAgentRepresentation(agent, false))
                .collect(Collectors.toList());
    }

    @GET
    @Path("/agents/instances/{id}")
    public Response getAgentInstance(@PathParam("id") String instanceId) {
        try {
            return Response.ok().entity(toAgentRepresentation(simulatedCloud.getAgentInstance(instanceId), true)).build();
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @GET
    @Path("/agents/containers")
    public List<SimulatedContainerRepresentation> getContainers() {
        return simulatedCloud.getAgentInstanceGroups().stream()
                .flatMap(g -> g.getAgents().stream())
                .flatMap(a -> a.getAllTasks().stream())
                .map(this::toContainerRepresentation)
                .collect(Collectors.toList());
    }

    @PUT
    @Path("/agents/instanceGroups/{id}/capacity")
    public Response updateAgentGroupCapacity(@PathParam("id") String agentGroupId, CapacityRepresentation capacity) {
        simulatedCloud.updateAgentGroupCapacity(agentGroupId, capacity.getMin(), capacity.getDesired(), capacity.getMax());
        return Response.noContent().build();
    }

    @PUT
    @Path("/agents/containers/{id}/state")
    public Response updateContainerState(@PathParam("id") String containerId, Protos.TaskState containerState) {
        TaskExecutorHolder container;
        try {
            container = simulatedCloud.getContainer(containerId);
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        container.transitionTo(containerState);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/agents/instanceGroups/{id}")
    public Response terminateAgentGroup(@PathParam("id") String agentGroupId) {
        simulatedCloud.removeInstanceGroup(agentGroupId);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/agents/instances/{id}")
    public Response terminateAgent(@PathParam("id") String agentId) {
        simulatedCloud.removeInstance(agentId);
        return Response.noContent().build();
    }

    private SimulatedAgentGroupRepresentation toAgentGroupRepresentation(SimulatedTitusAgentCluster agentInstanceGroup) {
        return new SimulatedAgentGroupRepresentation(
                agentInstanceGroup.getName(),
                agentInstanceGroup.getInstanceType(),
                agentInstanceGroup.getAgents().size(),
                agentInstanceGroup.getAgents().stream().map(agent -> toAgentRepresentation(agent, false)).collect(Collectors.toList())
        );
    }

    private SimulatedAgentRepresentation toAgentRepresentation(SimulatedTitusAgent agent, boolean withContainers) {
        return new SimulatedAgentRepresentation(
                agent.getId(),
                agent.getHostName(),
                agent.getLaunchTime(),
                withContainers ? agent.getAllTasks().stream().map(this::toContainerRepresentation).collect(Collectors.toList()) : Collections.emptyList()
        );
    }

    private SimulatedContainerRepresentation toContainerRepresentation(TaskExecutorHolder container) {
        return new SimulatedContainerRepresentation(
                container.getTaskId(),
                container.getJobId(),
                container.getState(),
                container.getTaskCPUs(),
                container.getTaskGPUs(),
                container.getTaskMem(),
                container.getTaskDisk(),
                container.getTaskNetworkMbs(),
                container.getContainerIp()
        );
    }
}
