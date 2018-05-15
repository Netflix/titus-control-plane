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

package com.netflix.titus.gateway.endpoint.v3.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.gateway.service.v3.SchedulerService;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Scheduler")
@Path("/v3/scheduler")
@Singleton
public class SchedulerResource {

    private final SchedulerService schedulerService;

    @Inject
    public SchedulerResource(SchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    @GET
    @ApiOperation("Get all system selectors")
    @Path("/systemSelectors")
    public SystemSelectors getSystemSelectors() {
        return Responses.fromSingleValueObservable(schedulerService.getSystemSelectors());
    }

    @GET
    @ApiOperation("Get a system selector with the given name")
    @Path("/systemSelectors/{id}")
    public SystemSelector getSystemSelector(@PathParam("id") String id) {
        return Responses.fromSingleValueObservable(schedulerService.getSystemSelector(id));
    }

    @POST
    @ApiOperation("Create a system selector")
    @Path("/systemSelectors")
    public Response createSystemSelector(SystemSelector systemSelector) {
        return Responses.fromCompletable(schedulerService.createSystemSelector(systemSelector));
    }

    @PUT
    @ApiOperation("Update a system selector")
    @Path("/systemSelectors/{id}")
    public Response updateSystemSelector(@PathParam("id") String id, SystemSelector systemSelector) {
        return Responses.fromCompletable(schedulerService.updateSystemSelector(id, systemSelector));
    }

    @DELETE
    @ApiOperation("Delete a system selector")
    @Path("/systemSelectors/{id}")
    public Response deleteSystemSelector(@PathParam("id") String id) {
        return Responses.fromCompletable(schedulerService.deleteSystemSelector(id));
    }

    @GET
    @ApiOperation("Find scheduling result for a task")
    @Path("/results/{id}")
    public SchedulingResultEvent findLastSchedulingResult(@PathParam("id") String taskId) {
        return Responses.fromSingleValueObservable(schedulerService.findLastSchedulingResult(taskId));
    }
}
