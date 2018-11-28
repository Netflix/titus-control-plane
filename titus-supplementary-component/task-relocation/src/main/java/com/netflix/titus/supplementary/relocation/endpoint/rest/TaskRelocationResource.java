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

package com.netflix.titus.supplementary.relocation.endpoint.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Task relocation")
@Singleton
@Path("/api/v3/relocation")
public class TaskRelocationResource {

    private final RelocationWorkflowExecutor relocationWorkflowExecutor;
    private final TaskRelocationResultStore archiveStore;

    @Inject
    public TaskRelocationResource(RelocationWorkflowExecutor relocationWorkflowExecutor,
                                  TaskRelocationResultStore archiveStore) {
        this.relocationWorkflowExecutor = relocationWorkflowExecutor;
        this.archiveStore = archiveStore;
    }

    @GET
    @Path("/plans")
    @ApiOperation("Get all active relocation plans")
    public TaskRelocationPlans getCurrentTaskRelocationPlans() {
        List<TaskRelocationPlan> corePlans = new ArrayList<>(relocationWorkflowExecutor.getPlannedRelocations().values());
        return RelocationGrpcModelConverters.toGrpcTaskRelocationPlans(corePlans);
    }

    @GET
    @Path("/plans/{taskId}")
    @ApiOperation("Get all active relocation plans")
    public com.netflix.titus.grpc.protogen.TaskRelocationPlan getTaskRelocationPlan(@PathParam("taskId") String taskId) {
        TaskRelocationPlan plan = relocationWorkflowExecutor.getPlannedRelocations().get(taskId);
        if (plan != null) {
            return RelocationGrpcModelConverters.toGrpcTaskRelocationPlan(plan);
        }
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
    }

    @GET
    @Path("/executions")
    @ApiOperation("Get task relocation execution results")
    public TaskRelocationExecutions getTaskRelocationResults() {
        List<TaskRelocationStatus> coreResults = new ArrayList<>(relocationWorkflowExecutor.getLastEvictionResults().values());
        return RelocationGrpcModelConverters.toGrpcTaskRelocationExecutions(coreResults);
    }

    @GET
    @Path("/executions/{taskId}")
    @ApiOperation("Get task relocation execution results")
    public TaskRelocationExecution getTaskRelocationResult(@PathParam("taskId") String taskId) {
        TaskRelocationStatus latest = relocationWorkflowExecutor.getLastEvictionResults().get(taskId);
        List<TaskRelocationStatus> archived = archiveStore.getTaskRelocationStatusList(taskId).block();

        if (latest == null && archived.isEmpty()) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

        List<TaskRelocationStatus> combined;
        if (latest == null) {
            combined = archived;
        } else if (archived.isEmpty()) {
            combined = Collections.singletonList(latest);
        } else {
            if (CollectionsExt.last(archived).equals(latest)) {
                combined = archived;
            } else {
                combined = CollectionsExt.copyAndAdd(archived, latest);
            }
        }

        return RelocationGrpcModelConverters.toGrpcTaskRelocationExecution(combined);
    }
}
