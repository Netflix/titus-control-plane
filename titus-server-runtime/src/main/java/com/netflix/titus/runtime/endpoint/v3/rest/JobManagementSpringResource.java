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

import com.google.common.base.Strings;
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
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
@RequestMapping(path = "/api/v3", produces = "application/json")
public class JobManagementSpringResource {

    private final JobServiceGateway jobServiceGateway;
    private final SystemLogService systemLog;

    @Inject
    public JobManagementSpringResource(JobServiceGateway jobServiceGateway, SystemLogService systemLog) {
        this.jobServiceGateway = jobServiceGateway;
        this.systemLog = systemLog;
    }

    @ApiOperation("Create a job")
    @PostMapping(path = "/jobs")
    public ResponseEntity<JobId> createJob(@RequestBody JobDescriptor jobDescriptor, CallMetadataAuthentication authentication) {
        String jobId = Responses.fromSingleValueObservable(jobServiceGateway.createJob(jobDescriptor, authentication.getCallMetadata()));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(JobId.newBuilder().setId(jobId).build());
    }

    @ApiOperation("Update an existing job's capacity")
    @PutMapping(path = "/jobs/{jobId}/instances")
    public ResponseEntity<Void> setInstances(@PathVariable("jobId") String jobId,
                                             @RequestBody Capacity capacity,
                                             CallMetadataAuthentication authentication) {
        JobCapacityUpdate jobCapacityUpdate = JobCapacityUpdate.newBuilder()
                .setJobId(jobId)
                .setCapacity(capacity)
                .build();
        return Responses.fromCompletable(
                jobServiceGateway.updateJobCapacity(jobCapacityUpdate, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update an existing job's capacity. Optional attributes min / max / desired are supported.")
    @PutMapping(path = "/jobs/{jobId}/capacityAttributes")
    public ResponseEntity<Void> setCapacityWithOptionalAttributes(@PathVariable("jobId") String jobId,
                                                                  @RequestBody JobCapacityWithOptionalAttributes capacity,
                                                                  CallMetadataAuthentication authentication) {
        JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes = JobCapacityUpdateWithOptionalAttributes.newBuilder()
                .setJobId(jobId)
                .setJobCapacityWithOptionalAttributes(capacity)
                .build();
        return Responses.fromCompletable(
                jobServiceGateway.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update an existing job's processes")
    @PutMapping(path = "/jobs/{jobId}/jobprocesses")
    public ResponseEntity<Void> setJobProcesses(@PathVariable("jobId") String jobId,
                                                @RequestBody ServiceJobSpec.ServiceJobProcesses jobProcesses,
                                                CallMetadataAuthentication authentication) {
        JobProcessesUpdate jobProcessesUpdate = JobProcessesUpdate.newBuilder()
                .setJobId(jobId)
                .setServiceJobProcesses(jobProcesses)
                .build();
        return Responses.fromCompletable(
                jobServiceGateway.updateJobProcesses(jobProcessesUpdate, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update job's disruption budget")
    @PutMapping(path = "/jobs/{jobId}/disruptionBudget")
    public ResponseEntity<Void> setJobDisruptionBudget(@PathVariable("jobId") String jobId,
                                                       @RequestBody JobDisruptionBudget jobDisruptionBudget,
                                                       CallMetadataAuthentication authentication) {
        JobDisruptionBudgetUpdate request = JobDisruptionBudgetUpdate.newBuilder()
                .setJobId(jobId)
                .setDisruptionBudget(jobDisruptionBudget)
                .build();
        return Responses.fromVoidMono(
                jobServiceGateway.updateJobDisruptionBudget(request, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update attributes of a job")
    @PutMapping(path = "/jobs/{jobId}/attributes")
    public ResponseEntity<Void> updateJobAttributes(@PathVariable("jobId") String jobId,
                                                    @RequestBody JobAttributesUpdate request,
                                                    CallMetadataAuthentication authentication) {
        JobAttributesUpdate sanitizedRequest;
        if (request.getJobId().isEmpty()) {
            sanitizedRequest = request.toBuilder().setJobId(jobId).build();
        } else {
            if (!jobId.equals(request.getJobId())) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            sanitizedRequest = request;
        }
        return Responses.fromVoidMono(
                jobServiceGateway.updateJobAttributes(sanitizedRequest, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Delete attributes of a job with the specified key names")
    @DeleteMapping(path = "/jobs/{jobId}/attributes")
    public ResponseEntity<Void> deleteJobAttributes(@PathVariable("jobId") String jobId,
                                                    @RequestParam("keys") String delimitedKeys,
                                                    CallMetadataAuthentication authentication) {
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
        return Responses.fromVoidMono(
                jobServiceGateway.deleteJobAttributes(request, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update an existing job's status")
    @PostMapping(path = "/jobs/{jobId}/enable")
    public ResponseEntity<Void> enableJob(@PathVariable("jobId") String jobId, CallMetadataAuthentication authentication) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(true)
                .build();
        return Responses.fromCompletable(
                jobServiceGateway.updateJobStatus(jobStatusUpdate, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Update an existing job's status")
    @PostMapping(path = "/jobs/{jobId}/disable")
    public ResponseEntity<Void> disableJob(@PathVariable("jobId") String jobId, CallMetadataAuthentication authentication) {
        JobStatusUpdate jobStatusUpdate = JobStatusUpdate.newBuilder()
                .setId(jobId)
                .setEnableStatus(false)
                .build();
        return Responses.fromCompletable(
                jobServiceGateway.updateJobStatus(jobStatusUpdate, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Find the job with the specified ID")
    @GetMapping(path = "/jobs/{jobId}", produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
    public Job findJob(@PathVariable("jobId") String jobId, CallMetadataAuthentication authentication) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findJob(jobId, authentication.getCallMetadata()));
    }

    @ApiOperation("Find jobs")
    @GetMapping(path = "/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
    public JobQueryResult findJobs(@RequestParam MultiValueMap<String, String> queryParameters, CallMetadataAuthentication authentication) {
        JobQuery.Builder queryBuilder = JobQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, authentication.getCallMetadata(), getClass().getSimpleName(), "findJobs", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findJobs(queryBuilder.build(), authentication.getCallMetadata()));
    }

    @ApiOperation("Kill a job")
    @DeleteMapping(path = "/jobs/{jobId}")
    public ResponseEntity<Void> killJob(@PathVariable("jobId") String jobId, CallMetadataAuthentication authentication) {
        return Responses.fromCompletable(jobServiceGateway.killJob(jobId, authentication.getCallMetadata()), HttpStatus.OK);
    }

    @ApiOperation(value = "Find a task with the specified ID", notes = "Returns an exact task if you already know the task ID", response = Job.class)
    @GetMapping(path = "/tasks/{taskId}")
    public Task findTask(@PathVariable("taskId") String taskId, CallMetadataAuthentication authentication) {
        return Responses.fromSingleValueObservable(jobServiceGateway.findTask(taskId, authentication.getCallMetadata()));
    }

    @ApiOperation(value = "Find tasks", notes = "Find tasks with optional filtering criteria")
    @GetMapping(path = "/tasks")
    public TaskQueryResult findTasks(@RequestParam MultiValueMap<String, String> queryParameters, CallMetadataAuthentication authentication) {
        TaskQuery.Builder queryBuilder = TaskQuery.newBuilder();
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, authentication.getCallMetadata(), getClass().getSimpleName(), "findTasks", page);
        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));
        return Responses.fromSingleValueObservable(jobServiceGateway.findTasks(queryBuilder.build(), authentication.getCallMetadata()));
    }

    @ApiOperation(value = "Kill task", notes = "Terminates a Titus Task given a particular Task ID.")
    @DeleteMapping(path = "/tasks/{taskId}")
    public ResponseEntity<Void> killTask(
            @ApiParam(name = "taskId", value = "Titus Task ID you which to terminate")
            @PathVariable("taskId") String taskId,
            @ApiParam(name = "shrink", value = "Set true to shrink the desired of the job after killing the task. Defaults to false.", defaultValue = "false")
            @RequestParam(name = "shrink", defaultValue = "false") boolean shrink,
            @ApiParam(name = "preventMinSizeUpdate", value = "Set true to prevent the job from going below its minimum size. Defaults to false.", defaultValue = "false")
            @RequestParam(name = "preventMinSizeUpdate", defaultValue = "false") boolean preventMinSizeUpdate,
            CallMetadataAuthentication authentication
    ) {
        TaskKillRequest taskKillRequest = TaskKillRequest.newBuilder()
                .setTaskId(taskId)
                .setShrink(shrink)
                .setPreventMinSizeUpdate(preventMinSizeUpdate)
                .build();
        return Responses.fromCompletable(jobServiceGateway.killTask(taskKillRequest, authentication.getCallMetadata()), HttpStatus.OK);
    }

    @ApiOperation("Update attributes of a task")
    @PutMapping(path = "/tasks/{taskId}/attributes")
    public ResponseEntity<Void> updateTaskAttributes(@PathVariable("taskId") String taskId,
                                                     @RequestBody TaskAttributesUpdate request,
                                                     CallMetadataAuthentication authentication) {
        TaskAttributesUpdate sanitizedRequest;
        if (request.getTaskId().isEmpty()) {
            sanitizedRequest = request.toBuilder().setTaskId(taskId).build();
        } else {
            if (!taskId.equals(request.getTaskId())) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            sanitizedRequest = request;
        }
        return Responses.fromCompletable(
                jobServiceGateway.updateTaskAttributes(sanitizedRequest, authentication.getCallMetadata()),
                HttpStatus.OK
        );
    }

    @ApiOperation("Delete attributes of a task with the specified key names")
    @DeleteMapping(path = "/tasks/{taskId}/attributes")
    public ResponseEntity<Void> deleteTaskAttributes(@PathVariable("taskId") String taskId,
                                                     @RequestParam("keys") String delimitedKeys,
                                                     CallMetadataAuthentication authentication) {
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
        return Responses.fromCompletable(jobServiceGateway.deleteTaskAttributes(request, authentication.getCallMetadata()), HttpStatus.OK);
    }

    @ApiOperation("Move task to another job")
    @PostMapping(path = "/tasks/move")
    public ResponseEntity<Void> moveTask(@RequestBody TaskMoveRequest taskMoveRequest, CallMetadataAuthentication authentication) {
        return Responses.fromCompletable(jobServiceGateway.moveTask(taskMoveRequest, authentication.getCallMetadata()), HttpStatus.OK);
    }
}
