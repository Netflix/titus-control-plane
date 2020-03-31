/*
 * Copyright 2020 Netflix, Inc.
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
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Api(tags = "Job Management")
@RestController
@RequestMapping(path = "/api/v3")
public class JobManagementSpringResource {

    private final JobServiceGateway jobServiceGateway;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public JobManagementSpringResource(JobServiceGateway jobServiceGateway,
                                       SystemLogService systemLog,
                                       CallMetadataResolver callMetadataResolver) {
        this.jobServiceGateway = jobServiceGateway;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @ApiOperation("Create a job")
    @PostMapping(path = "/jobs")
    public Response createJob(JobDescriptor jobDescriptor) {
        String jobId = Responses.fromSingleValueObservable(jobServiceGateway.createJob(jobDescriptor, resolveCallMetadata()));
        return Response.status(Response.Status.ACCEPTED).entity(JobId.newBuilder().setId(jobId).build()).build();
    }

    @ApiOperation("Update an existing job's capacity")
    @PutMapping(path = "/jobs/{jobId}/instances")
    public Response setInstances(@PathVariable("jobId") String jobId,
                                 Capacity capacity) {
        JobCapacityUpdate jobCapacityUpdate = JobCapacityUpdate.newBuilder()
                .setJobId(jobId)
                .setCapacity(capacity)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobCapacity(jobCapacityUpdate));
    }

    @ApiOperation("Update an existing job's capacity. Optional attributes min / max / desired are supported.")
    @PutMapping(path = "/jobs/{jobId}/capacityAttributes")
    public Response setCapacityWithOptionalAttributes(@PathVariable("jobId") String jobId,
                                                      JobCapacityWithOptionalAttributes capacity) {
        JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes = JobCapacityUpdateWithOptionalAttributes.newBuilder()
                .setJobId(jobId)
                .setJobCapacityWithOptionalAttributes(capacity)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes));
    }

    @ApiOperation("Update an existing job's processes")
    @PutMapping(path = "/jobs/{jobId}/jobprocesses")
    public Response setJobProcesses(@PathVariable("jobId") String jobId,
                                    ServiceJobSpec.ServiceJobProcesses jobProcesses) {
        JobProcessesUpdate jobProcessesUpdate = JobProcessesUpdate.newBuilder()
                .setJobId(jobId)
                .setServiceJobProcesses(jobProcesses)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobProcesses(jobProcessesUpdate));
    }

    @ApiOperation("Update job's disruption budget")
    @PutMapping(path = "/jobs/{jobId}/disruptionBudget")
    public Response setJobDisruptionBudget(@PathVariable("jobId") String jobId,
                                           JobDisruptionBudget jobDisruptionBudget) {
        JobDisruptionBudgetUpdate request = JobDisruptionBudgetUpdate.newBuilder()
                .setJobId(jobId)
                .setDisruptionBudget(jobDisruptionBudget)
                .build();
        return Responses.fromVoidMono(jobServiceGateway.updateJobDisruptionBudget(request));
    }

    @ApiOperation("Update attributes of a job")
    @PutMapping(path = "/jobs/{jobId}/attributes")
    public Response updateJobAttributes(@PathVariable("jobId") String jobId,
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
        return Responses.fromVoidMono(jobServiceGateway.updateJobAttributes(sanitizedRequest));
    }

    @ApiOperation("Delete attributes of a job with the specified key names")
    @DeleteMapping(path = "/jobs/{jobId}/attributes")
    public Response deleteJobAttributes(@PathVariable("jobId") String jobId, @RequestParam("keys") String delimitedKeys) {
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
        return Responses.fromVoidMono(jobServiceGateway.deleteJobAttributes(request));
    }

    @ApiOperation("Update an existing job's status")
    @PostMapping(path = "/jobs/{jobId}/enable")
    public Response enableJob(@PathVariable("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(true)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobStatus(jobStatusUpdate));
    }

    @ApiOperation("Update an existing job's status")
    @PostMapping(path = "/jobs/{jobId}/disable")
    public Response disableJob(@PathVariable("jobId") String jobId) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(false)
                .build();
        return Responses.fromCompletable(jobServiceGateway.updateJobStatus(jobStatusUpdate));
    }

    @ApiOperation("Find the job with the specified ID")
    @GetMapping(path = "/jobs/{jobId}", produces = MediaType.APPLICATION_JSON)
    public Response findJob(@PathVariable("jobId") String jobId) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findJob(jobId));
    }

    @ApiOperation("Find jobs")
    @GetMapping(path = "/jobs", produces = MediaType.APPLICATION_JSON)
    public JobQueryResult findJobs(@RequestParam MultiValueMap<String, String> queryParameters) {
        JobQuery.Builder queryBuilder = JobQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findJobs", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findJobs(queryBuilder.build()));
    }

    @ApiOperation("Kill a job")
    @DeleteMapping(path = "/jobs/{jobId}")
    public Response killJob(@PathVariable("jobId") String jobId) {
        return Responses.fromCompletable(jobServiceGateway.killJob(jobId));
    }

    @ApiOperation("Find the task with the specified ID")
    @GetMapping(path = "/tasks/{taskId}")
    public Task findTask(@PathVariable("taskId") String taskId) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findTask(taskId));
    }

    @ApiOperation("Find tasks")
    @GetMapping(path = "/tasks")
    public TaskQueryResult findTasks(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);
        TaskQuery.Builder queryBuilder = TaskQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "findTasks", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findTasks(queryBuilder.build()));
    }

    @ApiOperation("Kill task")
    @DeleteMapping(path = "/tasks/{taskId}")
    public Response killTask(
            @PathVariable("taskId") String taskId,
            @RequestParam(name = "shrink", defaultValue = "false") boolean shrink,
            @RequestParam(name = "preventMinSizeUpdate", defaultValue = "false") boolean preventMinSizeUpdate
    ) {
        TaskKillRequest taskKillRequest = TaskKillRequest.newBuilder()
                .setTaskId(taskId)
                .setShrink(shrink)
                .setPreventMinSizeUpdate(preventMinSizeUpdate)
                .build();
        return Responses.fromCompletable(jobServiceGateway.killTask(taskKillRequest));
    }

    @ApiOperation("Update attributes of a task")
    @PutMapping(path = "/tasks/{taskId}/attributes")
    public Response updateTaskAttributes(@PathVariable("taskId") String taskId,
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
        return Responses.fromCompletable(jobServiceGateway.updateTaskAttributes(sanitizedRequest));
    }

    @ApiOperation("Delete attributes of a task with the specified key names")
    @DeleteMapping(path = "/tasks/{taskId}/attributes")
    public Response deleteTaskAttributes(@PathVariable("taskId") String taskId, @RequestParam("keys") String delimitedKeys) {
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
        return Responses.fromCompletable(jobServiceGateway.deleteTaskAttributes(request));
    }

    @ApiOperation("Move task to another job")
    @PostMapping(path = "/tasks/move")
    public Response moveTask(@RequestBody TaskMoveRequest taskMoveRequest) {
        return Responses.fromCompletable(jobServiceGateway.moveTask(taskMoveRequest));
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}
