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

import java.util.Set;
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

import com.google.common.base.Strings;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudget;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Path("/v3")
@Singleton
public class JobManagementResource {

    private final JobManagementClient jobManagementClient;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public JobManagementResource(JobManagementClient jobManagementClient,
                                 SystemLogService systemLog,
                                 CallMetadataResolver callMetadataResolver) {
        this.jobManagementClient = jobManagementClient;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @POST
    @ApiOperation("Create a job")
    @Path("/jobs")
    public Response createJob(JobDescriptor jobDescriptor) {
        String jobId = Responses.fromSingleValueObservable(jobManagementClient.createJob(jobDescriptor));
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
        return Responses.fromCompletable(jobManagementClient.updateJobCapacity(jobCapacityUpdate));
    }

    @PUT
    @ApiOperation("Update an existing job's processes")
    @Path("/jobs/{jobId}/jobprocesses")
    public Response setJobProcesses(@PathParam("jobId") String jobId,
                                    ServiceJobSpec.ServiceJobProcesses jobProcesses) {
        JobProcessesUpdate jobProcessesUpdate = JobProcessesUpdate.newBuilder()
                .setJobId(jobId)
                .setServiceJobProcesses(jobProcesses)
                .build();
        return Responses.fromCompletable(jobManagementClient.updateJobProcesses(jobProcessesUpdate));
    }

    @PUT
    @ApiOperation("Update job's disruption budget")
    @Path("/jobs/{jobId}/disruptionBudget")
    public Response setJobDisruptionBudget(@PathParam("jobId") String jobId,
                                           JobDisruptionBudget jobDisruptionBudget) {
        JobDisruptionBudgetUpdate request = JobDisruptionBudgetUpdate.newBuilder()
                .setJobId(jobId)
                .setDisruptionBudget(jobDisruptionBudget)
                .build();
        return Responses.fromVoidMono(jobManagementClient.updateJobDisruptionBudget(request));
    }

    @PUT
    @ApiOperation("Update attributes of a job")
    @Path("/jobs/{jobId}/attributes")
    public Response updateJobAttributes(@PathParam("jobId") String jobId,
                                        JobAttributesUpdate request) {
        JobAttributesUpdate sanitizedRequest;
        if (request.getJobId().isEmpty()) {
            sanitizedRequest = request.toBuilder().setJobId(jobId).build();
        } else {
            if (!jobId.equals(request.getJobId())) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            sanitizedRequest = request;
        }
        return Responses.fromVoidMono(jobManagementClient.updateJobAttributes(sanitizedRequest));
    }

    @DELETE
    @ApiOperation("Delete attributes of a job with the specified key names")
    @Path("/jobs/{jobId}/attributes")
    public Response deleteJobAttributes(@PathParam("jobId") String jobId, @QueryParam("keys") String delimitedKeys) {
        if (Strings.isNullOrEmpty(delimitedKeys)) {
            throw TitusServiceException.invalidArgument("Path parameter 'keys' cannot be empty");
        }

        Set<String> keys = StringExt.splitByCommaIntoSet(delimitedKeys);
        if (keys.isEmpty()) {
            throw TitusServiceException.invalidArgument("Parsed path parameter 'keys' cannot be empty");
        }

        JobAttributesDeleteRequest request = JobAttributesDeleteRequest.newBuilder()
                .setJobId(jobId)
                .addAllKeys(keys)
                .build();
        return Responses.fromVoidMono(jobManagementClient.deleteJobAttributes(request));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/enable")
    public Response enableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(true)
                .build();
        return Responses.fromCompletable(jobManagementClient.updateJobStatus(jobStatusUpdate));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/disable")
    public Response disableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(false)
                .build();
        return Responses.fromCompletable(jobManagementClient.updateJobStatus(jobStatusUpdate));
    }

    @GET
    @ApiOperation("Find the job with the specified ID")
    @Path("/jobs/{jobId}")
    public Job findJob(@PathParam("jobId") String jobId) {
        return Responses.fromSingleValueObservable(jobManagementClient.findJob(jobId));
    }

    @GET
    @ApiOperation("Find jobs")
    @Path("/jobs")
    public JobQueryResult findJobs(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        JobQuery.Builder queryBuilder = JobQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findJobs", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobManagementClient.findJobs(queryBuilder.build()));
    }

    @DELETE
    @ApiOperation("Kill a job")
    @Path("/jobs/{jobId}")
    public Response killJob(@PathParam("jobId") String jobId) {
        return Responses.fromCompletable(jobManagementClient.killJob(jobId));
    }

    @GET
    @ApiOperation("Find the task with the specified ID")
    @Path("/tasks/{taskId}")
    public Task findTask(@PathParam("taskId") String taskId) {
        return Responses.fromSingleValueObservable(jobManagementClient.findTask(taskId));
    }

    @GET
    @ApiOperation("Find tasks")
    @Path("/tasks")
    public TaskQueryResult findTasks(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        TaskQuery.Builder queryBuilder = TaskQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findTasks", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobManagementClient.findTasks(queryBuilder.build()));
    }

    @DELETE
    @ApiOperation("Kill task")
    @Path("/tasks/{taskId}")
    public Response killTask(
            @PathParam("taskId") String taskId,
            @DefaultValue("false") @QueryParam("shrink") boolean shrink
    ) {
        TaskKillRequest taskKillRequest = TaskKillRequest.newBuilder().setTaskId(taskId).setShrink(shrink).build();
        return Responses.fromCompletable(jobManagementClient.killTask(taskKillRequest));
    }

    @PUT
    @ApiOperation("Update attributes of a task")
    @Path("/tasks/{taskId}/attributes")
    public Response updateTaskAttributes(@PathParam("taskId") String taskId,
                                         TaskAttributesUpdate request) {
        TaskAttributesUpdate sanitizedRequest;
        if (request.getTaskId().isEmpty()) {
            sanitizedRequest = request.toBuilder().setTaskId(taskId).build();
        } else {
            if (!taskId.equals(request.getTaskId())) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            sanitizedRequest = request;
        }
        return Responses.fromCompletable(jobManagementClient.updateTaskAttributes(sanitizedRequest));
    }

    @DELETE
    @ApiOperation("Delete attributes of a task with the specified key names")
    @Path("/tasks/{taskId}/attributes")
    public Response deleteTaskAttributes(@PathParam("taskId") String taskId, @QueryParam("keys") String delimitedKeys) {
        if (Strings.isNullOrEmpty(delimitedKeys)) {
            throw TitusServiceException.invalidArgument("Path parameter 'keys' cannot be empty");
        }

        Set<String> keys = StringExt.splitByCommaIntoSet(delimitedKeys);
        if (keys.isEmpty()) {
            throw TitusServiceException.invalidArgument("Parsed path parameter 'keys' cannot be empty");
        }

        TaskAttributesDeleteRequest request = TaskAttributesDeleteRequest.newBuilder()
                .setTaskId(taskId)
                .addAllKeys(keys)
                .build();
        return Responses.fromCompletable(jobManagementClient.deleteTaskAttributes(request));
    }

    @POST
    @ApiOperation("Move task to another job")
    @Path("/tasks/move")
    public Response moveTask(TaskMoveRequest taskMoveRequest) {
        return Responses.fromCompletable(jobManagementClient.moveTask(taskMoveRequest));
    }
}
