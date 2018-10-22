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
import java.util.List;
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
import com.netflix.titus.supplementary.relocation.endpoint.grpc.RelocationGrpcModelConverters;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Job Management")
@Singleton
@Path("/api/v3/relocation")
public class TaskRelocationResource {

    private final RelocationWorkflowExecutor relocationWorkflowExecutor;

    @Inject
    public TaskRelocationResource(RelocationWorkflowExecutor relocationWorkflowExecutor) {
        this.relocationWorkflowExecutor = relocationWorkflowExecutor;
    }

    @GET
    @Path("/plans")
    @ApiOperation("Get all active relocation plans")
    public TaskRelocationPlans getCurrentTaskRelocationPlans(@Context UriInfo info) {
        List<TaskRelocationPlan> corePlans = new ArrayList<>(relocationWorkflowExecutor.getPlannedRelocations().values());
        return RelocationGrpcModelConverters.toGrpcTaskRelocationPlans(corePlans);
    }

    @GET
    @Path("/executions")
    @ApiOperation("Get task relocation execution results")
    public TaskRelocationExecutions getTaskRelocationResult(@Context UriInfo info) {
        List<TaskRelocationStatus> coreResults = new ArrayList<>(relocationWorkflowExecutor.getLastEvictionResults().values());
        return RelocationGrpcModelConverters.toGrpcTaskRelocationExecutions(coreResults);
    }
}
