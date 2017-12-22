package io.netflix.titus.testkit.embedded.cloud.endpoint.rest;

import java.util.HashSet;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import com.sun.jersey.spi.resource.Singleton;
import io.netflix.titus.testkit.embedded.cloud.endpoint.SimulatedCloudGateway;

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

    @GET
    @Path("/instanceGroups/{instanceGroupId}/instances")
    public List<SimulatedInstance> getInstances(@PathParam("instanceGroupId") String instanceGroupId) {
        return gateway.getInstances(instanceGroupId);
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
