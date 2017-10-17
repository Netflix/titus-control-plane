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

package io.netflix.titus.gateway.endpoint.v3.rest;

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
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.netflix.titus.gateway.service.v3.JobManagementService;
import io.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Path("/v3")
@Singleton
public class JobManagementResource {

    private final JobManagementService jobManagementService;

    @Inject
    public JobManagementResource(JobManagementService jobManagementService) {
        this.jobManagementService = jobManagementService;
    }

    @POST
    @ApiOperation("Create a job")
    @Path("/jobs")
    public Response createJob(JobDescriptor jobDescriptor) {
        String jobId = Responses.fromSingleValueObservable(jobManagementService.createJob(jobDescriptor));
        return Response.status(Response.Status.ACCEPTED).entity(JobId.newBuilder().setId(jobId).build()).build();
    }

    @PUT
    @ApiOperation("Update an existing job's capacity")
    @Path("/jobs/{jobId}/instances")
    public Response setInstances(@PathParam("jobId") String jobId,
                                 Capacity capacity) {
        JobCapacityUpdate jobCapacityUpdate = JobCapacityUpdate.newBuilder()
                .setJobId(jobId)
                .setCapacity(capacity)
                .build();
        return Responses.fromCompletable(jobManagementService.updateJobCapacity(jobCapacityUpdate));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/enable")
    public Response enableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(true)
                .build();
        return Responses.fromCompletable(jobManagementService.changeJobInServiceStatus(jobStatusUpdate));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/disable")
    public Response disableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(false)
                .build();
        return Responses.fromCompletable(jobManagementService.changeJobInServiceStatus(jobStatusUpdate));
    }

    @GET
    @ApiOperation("Find the job with the specified ID")
    @Path("/jobs/{jobId}")
    public Job findJob(@PathParam("jobId") String jobId) {
        return Responses.fromSingleValueObservable(jobManagementService.findJob(jobId));
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
        return Responses.fromSingleValueObservable(jobManagementService.findJobs(queryBuilder.build()));
    }

    @DELETE
    @ApiOperation("Kill a job")
    @Path("/jobs/{jobId}")
    public Response killJob(@PathParam("jobId") String jobId) {
        return Responses.fromCompletable(jobManagementService.killJob(jobId));
    }

    @GET
    @ApiOperation("Find the task with the specified ID")
    @Path("/tasks/{taskId}")
    public Task findTask(@PathParam("taskId") String taskId) {
        return Responses.fromSingleValueObservable(jobManagementService.findTask(taskId));
    }

    @GET
    @ApiOperation("Find tasks")
    @Path("/tasks")
    public TaskQueryResult findTasks(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        TaskQuery.Builder queryBuilder = TaskQuery.newBuilder();
        queryBuilder.setPage(RestUtil.createPage(queryParameters));
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobManagementService.findTasks(queryBuilder.build()));
    }

    @DELETE
    @ApiOperation("Kill task")
    @Path("/tasks/{taskId}")
    public Response killTask(
            @PathParam("taskId") String taskId,
            @DefaultValue("false") @QueryParam("shrink") boolean shrink
    ) {
        TaskKillRequest taskKillRequest = TaskKillRequest.newBuilder().setTaskId(taskId).setShrink(shrink).build();
        return Responses.fromCompletable(jobManagementService.killTask(taskKillRequest));
    }
}
