package com.netflix.titus.supplementary.relocation.endpoint.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.supplementary.relocation.endpoint.StubRequestReplies;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Singleton
@Path("/api/v3/relocation")
public class TaskRelocationResource {

    @Inject
    public TaskRelocationResource() {
    }

    @GET
    @Path("/plans")
    @ApiOperation("Get all active relocation plans")
    public TaskRelocationPlans getCurrentTaskRelocationPlans(@Context UriInfo info) {
        return StubRequestReplies.STUB_RELOCATION_PLANS;
    }

    @GET
    @Path("/executions")
    @ApiOperation("Get task relocation execution results")
    public TaskRelocationExecutions getTaskRelocationResult(@Context UriInfo info) {
        return StubRequestReplies.STUB_RELOCATION_EXECUTIONS;
    }
}
