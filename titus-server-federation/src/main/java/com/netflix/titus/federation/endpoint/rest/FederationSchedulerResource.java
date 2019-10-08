/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.federation.endpoint.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.federation.service.AggregatingSchedulerService;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * Supports subset of operations provided by {@link com.netflix.titus.grpc.protogen.SchedulerServiceGrpc}, applicable
 * at the federation level.
 */
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Scheduler")
@Path("/v3/scheduler")
@Singleton
public class FederationSchedulerResource {

    private final AggregatingSchedulerService schedulerService;

    @Inject
    public FederationSchedulerResource(AggregatingSchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    @GET
    @ApiOperation("Find scheduling result for a task")
    @Path("/results/{id}")
    public SchedulingResultEvent findLastSchedulingResult(@PathParam("id") String taskId) {
        return Responses.fromMono(schedulerService.findLastSchedulingResult(taskId));
    }
}
