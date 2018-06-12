package com.netflix.titus.runtime.endpoint.v3.rest;

import com.netflix.titus.grpc.protogen.*;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Load Balancing")
@Path("/v3/loadBalancers")
@Singleton
public class LoadBalancerResource {
    private final LoadBalancerService loadBalancerService;

    @Inject
    public LoadBalancerResource(LoadBalancerService loadBalancerService) {
        this.loadBalancerService = loadBalancerService;
    }

    @GET
    @ApiOperation("Find the load balancer(s) with the specified ID")
    @Path("/{jobId}")
    public GetJobLoadBalancersResult getJobLoadBalancers(@PathParam("jobId") String jobId) {
        return Responses.fromSingleValueObservable(loadBalancerService.getLoadBalancers(
                JobId.newBuilder()
                        .setId(jobId)
                        .build()));
    }

    @GET
    @ApiOperation("Get all load balancers")
    public GetAllLoadBalancersResult getAllLoadBalancers(@Context UriInfo info) {
        return Responses.fromSingleValueObservable(
                loadBalancerService.getAllLoadBalancers(
                        GetAllLoadBalancersRequest.newBuilder()
                                .setPage(RestUtil.createPage(info.getQueryParameters()))
                                .build()));
    }

    @POST
    @ApiOperation("Add a load balancer")
    public Response addLoadBalancer(
            @QueryParam("jobId") String jobId,
            @QueryParam("loadBalancerId") String loadBalancerId) {
        return Responses.fromCompletable(loadBalancerService.addLoadBalancer(
                AddLoadBalancerRequest.newBuilder()
                        .setJobId(jobId)
                        .setLoadBalancerId(LoadBalancerId.newBuilder().setId(loadBalancerId).build())
                        .build()));
    }

    @DELETE
    @ApiOperation("Remove a load balancer")
    public Response removeLoadBalancer(
            @QueryParam("jobId") String jobId,
            @QueryParam("loadBalancerId") String loadBalancerId) {
        return Responses.fromCompletable(loadBalancerService.removeLoadBalancer(
                RemoveLoadBalancerRequest.newBuilder()
                        .setJobId(jobId)
                        .setLoadBalancerId(LoadBalancerId.newBuilder().setId(loadBalancerId).build())
                        .build()));
    }
}
