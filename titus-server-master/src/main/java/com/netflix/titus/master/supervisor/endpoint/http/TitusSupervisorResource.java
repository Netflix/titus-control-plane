package com.netflix.titus.master.supervisor.endpoint.http;

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters;
import com.netflix.titus.master.supervisor.service.SupervisorOperations;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Titus Supervisor")
@Path("/v3")
@Singleton
public class TitusSupervisorResource {

    private final SupervisorOperations supervisorOperations;

    @Inject
    public TitusSupervisorResource(SupervisorOperations supervisorOperations) {
        this.supervisorOperations = supervisorOperations;
    }

    @GET
    @ApiOperation("Find the TitusMaster instance with the specified id")
    @Path("/supervisor/instances")
    public List<MasterInstance> getAllMasterInstances() {
        return supervisorOperations.getMasterInstances().stream()
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .collect(Collectors.toList());
    }

    @GET
    @ApiOperation("Find the TitusMaster instance with the specified id")
    @Path("/supervisor/instances/{masterInstanceId}")
    public MasterInstance getMasterInstance(@PathParam("masterInstanceId") String masterInstanceId) {
        return supervisorOperations.findMasterInstance(masterInstanceId)
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .orElseThrow(() -> new WebApplicationException(Response.Status.NOT_FOUND));
    }

    @GET
    @ApiOperation("Find the TitusMaster instance with the specified id")
    @Path("/supervisor/instances/leader")
    public MasterInstance getLeader() {
        return supervisorOperations.findLeader()
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .orElseThrow(() -> new WebApplicationException(Response.Status.NOT_FOUND));
    }
}
