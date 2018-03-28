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

package io.netflix.titus.federation.endpoint.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.netflix.titus.federation.service.JobManagementService;
import io.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.netflix.titus.runtime.endpoint.v3.rest.RestUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Path("/v3")
@Singleton
public class JobManagementResource {

    private final JobManagementService service;

    @Inject
    public JobManagementResource(JobManagementService service) {
        this.service = service;
    }

    @POST
    @ApiOperation("Create a job")
    @Path("/jobs")
    public Response createJob(JobDescriptor jobDescriptor) {
        return Response.serverError().build();
    }

    @PUT
    @ApiOperation("Update an existing job's capacity")
    @Path("/jobs/{jobId}/instances")
    public Response setInstances(@PathParam("jobId") String jobId,
                                 Capacity capacity) {
        return Response.serverError().build();
    }

    @PUT
    @ApiOperation("Update an existing job's processes")
    @Path("/jobs/{jobId}/jobprocesses")
    public Response setJobProcesses(@PathParam("jobId") String jobId,
                                    ServiceJobSpec.ServiceJobProcesses jobProcesses) {
        return Response.serverError().build();
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/enable")
    public Response enableJob(@PathParam("jobId") String jobId) {
        return Response.serverError().build();
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/disable")
    public Response disableJob(@PathParam("jobId") String jobId) {
        return Response.serverError().build();
    }

    @GET
    @ApiOperation("Find the job with the specified ID")
    @Path("/jobs/{jobId}")
    public Job findJob(@PathParam("jobId") String jobId) {
        throw new IllegalStateException("Not implemented yet");
    }

    @GET
    @ApiOperation("Find jobs")
    @Path("/jobs")
    public JobQueryResult findJobs(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        JobQuery.Builder queryBuilder = JobQuery.newBuilder();
        queryBuilder.setPage(RestUtil.createPage(queryParameters));
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(service.findJobs(queryBuilder.build()));
    }

    @DELETE
    @ApiOperation("Kill a job")
    @Path("/jobs/{jobId}")
    public Response killJob(@PathParam("jobId") String jobId) {
        return Response.serverError().build();
    }

    @GET
    @ApiOperation("Find the task with the specified ID")
    @Path("/tasks/{taskId}")
    public Task findTask(@PathParam("taskId") String taskId) {
        throw new IllegalStateException("Not implemented yet");
    }

    @GET
    @ApiOperation("Find tasks")
    @Path("/tasks")
    public TaskQueryResult findTasks(@Context UriInfo info) {
        throw new IllegalStateException("Not implemented yet");
    }

    @DELETE
    @ApiOperation("Kill task")
    @Path("/tasks/{taskId}")
    public Response killTask(
            @PathParam("taskId") String taskId,
            @DefaultValue("false") @QueryParam("shrink") boolean shrink) {
        return Response.serverError().build();
    }
}
