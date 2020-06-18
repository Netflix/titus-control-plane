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

package com.netflix.titus.runtime.endpoint.v3.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Load Balancing")
@Path("/v3/loadBalancers")
@Singleton
public class LoadBalancerResource {
    private final LoadBalancerService loadBalancerService;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public LoadBalancerResource(LoadBalancerService loadBalancerService,
                                SystemLogService systemLog,
                                CallMetadataResolver callMetadataResolver) {
        this.loadBalancerService = loadBalancerService;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @GET
    @ApiOperation("Find the load balancer(s) with the specified ID")
    @Path("/{jobId}")
    public GetJobLoadBalancersResult getJobLoadBalancers(@PathParam("jobId") String jobId) {
        return Responses.fromSingleValueObservable(
                loadBalancerService.getLoadBalancers(JobId.newBuilder().setId(jobId).build(), resolveCallMetadata())
        );
    }

    @GET
    @ApiOperation("Get all load balancers")
    public GetAllLoadBalancersResult getAllLoadBalancers(@Context UriInfo info) {
        Page page = RestUtil.createPage(info.getQueryParameters());
        CallMetadata callMetadata = resolveCallMetadata();
        logPageNumberUsage(systemLog, callMetadata, getClass().getSimpleName(), "getAllLoadBalancers", page);
        return Responses.fromSingleValueObservable(
                loadBalancerService.getAllLoadBalancers(GetAllLoadBalancersRequest.newBuilder().setPage(page).build(), callMetadata)
        );
    }

    @POST
    @ApiOperation("Add a load balancer")
    public Response addLoadBalancer(
            @QueryParam("jobId") String jobId,
            @QueryParam("loadBalancerId") String loadBalancerId) {
        return Responses.fromCompletable(loadBalancerService.addLoadBalancer(
                AddLoadBalancerRequest.newBuilder().setJobId(jobId).setLoadBalancerId(LoadBalancerId.newBuilder().setId(loadBalancerId).build()).build(),
                resolveCallMetadata())
        );
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
                        .build(),
                resolveCallMetadata())
        );
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}
