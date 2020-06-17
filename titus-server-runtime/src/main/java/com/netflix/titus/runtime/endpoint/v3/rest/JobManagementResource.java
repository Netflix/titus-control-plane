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
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
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
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Path("/v3")
@Singleton
public class JobManagementResource {

    private final JobServiceGateway jobServiceGateway;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public JobManagementResource(JobServiceGateway jobServiceGateway,
                                 SystemLogService systemLog,
                                 CallMetadataResolver callMetadataResolver) {
        this.jobServiceGateway = jobServiceGateway;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @POST
    @ApiOperation("Create a job")
    @Path("/jobs")
    public Response createJob(JobDescriptor jobDescriptor) {
        String jobId = Responses.fromSingleValueObservable(jobServiceGateway.createJob(jobDescriptor, resolveCallMetadata()));
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
        return Responses.fromCompletable(jobServiceGateway.updateJobCapacity(jobCapacityUpdate, resolveCallMetadata()));
    }

    @PUT
    @ApiOperation("Update an existing job's capacity. Optional attributes min / max / desired are supported.")
    @Path("/jobs/{jobId}/capacityAttributes")
    public Response setCapacityWithOptionalAttributes(@PathParam("jobId") String jobId,
                                                      JobCapacityWithOptionalAttributes capacity) {
        JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes = JobCapacityUpdateWithOptionalAttributes.newBuilder()
                .setJobId(jobId)
                .setJobCapacityWithOptionalAttributes(capacity)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, resolveCallMetadata()));
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
        return Responses.fromCompletable(jobServiceGateway.updateJobProcesses(jobProcessesUpdate, resolveCallMetadata()));
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
        return Responses.fromVoidMono(jobServiceGateway.updateJobDisruptionBudget(request, resolveCallMetadata()));
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
        return Responses.fromVoidMono(jobServiceGateway.updateJobAttributes(sanitizedRequest, resolveCallMetadata()));
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
        return Responses.fromVoidMono(jobServiceGateway.deleteJobAttributes(request, resolveCallMetadata()));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/enable")
    public Response enableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(true)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobStatus(jobStatusUpdate, resolveCallMetadata()));
    }

    @POST
    @ApiOperation("Update an existing job's status")
    @Path("/jobs/{jobId}/disable")
    public Response disableJob(@PathParam("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(false)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobStatus(jobStatusUpdate, resolveCallMetadata()));
    }

    @GET
    @ApiOperation("Find the job with the specified ID")
    @Path("/jobs/{jobId}")
    public Job findJob(@PathParam("jobId") String jobId) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findJob(jobId, resolveCallMetadata()));
    }

    @GET
    @ApiOperation("Find jobs")
    @Path("/jobs")
    public JobQueryResult findJobs(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        JobQuery.Builder queryBuilder = JobQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        CallMetadata callMetadata = resolveCallMetadata();
        logPageNumberUsage(systemLog, callMetadata, getClass().getSimpleName(), "findJobs", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findJobs(queryBuilder.build(), callMetadata));
    }

    @DELETE
    @ApiOperation("Kill a job")
    @Path("/jobs/{jobId}")
    public Response killJob(@PathParam("jobId") String jobId) {
        return Responses.fromCompletable(jobServiceGateway.killJob(jobId, resolveCallMetadata()));
    }

    @GET
    @ApiOperation("Find the task with the specified ID")
    @Path("/tasks/{taskId}")
    public Task findTask(@PathParam("taskId") String taskId) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findTask(taskId, resolveCallMetadata()));
    }

    @GET
    @ApiOperation("Find tasks")
    @Path("/tasks")
    public TaskQueryResult findTasks(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        TaskQuery.Builder queryBuilder = TaskQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        CallMetadata callMetadata = resolveCallMetadata();
        logPageNumberUsage(systemLog, callMetadata, getClass().getSimpleName(), "findTasks", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findTasks(queryBuilder.build(), callMetadata));
    }

    @DELETE
    @ApiOperation("Kill task")
    @Path("/tasks/{taskId}")
    public Response killTask(
            @PathParam("taskId") String taskId,
            @DefaultValue("false") @QueryParam("shrink") boolean shrink,
            @DefaultValue("false") @QueryParam("preventMinSizeUpdate") boolean preventMinSizeUpdate
    ) {
        TaskKillRequest taskKillRequest = TaskKillRequest.newBuilder()
                .setTaskId(taskId)
                .setShrink(shrink)
                .setPreventMinSizeUpdate(preventMinSizeUpdate)
                .build();
        return Responses.fromCompletable(jobServiceGateway.killTask(taskKillRequest, resolveCallMetadata()));
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
        return Responses.fromCompletable(jobServiceGateway.updateTaskAttributes(sanitizedRequest, resolveCallMetadata()));
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
        return Responses.fromCompletable(jobServiceGateway.deleteTaskAttributes(request, resolveCallMetadata()));
    }

    @POST
    @ApiOperation("Move task to another job")
    @Path("/tasks/move")
    public Response moveTask(TaskMoveRequest taskMoveRequest) {
        return Responses.fromCompletable(jobServiceGateway.moveTask(taskMoveRequest, resolveCallMetadata()));
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}
