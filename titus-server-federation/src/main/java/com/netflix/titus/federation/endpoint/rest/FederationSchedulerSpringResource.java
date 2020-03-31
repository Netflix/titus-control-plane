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

package com.netflix.titus.federation.endpoint.rest;

import javax.inject.Inject;

import com.netflix.titus.federation.service.AggregatingSchedulerService;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Scheduler")
@RestController
@RequestMapping(path = "/api/v3/scheduler", produces = "application/json")
public class FederationSchedulerSpringResource {

    private final AggregatingSchedulerService schedulerService;

    @Inject
    public FederationSchedulerSpringResource(AggregatingSchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    @GetMapping(path = "/results/{id}")
    @ApiOperation("Find scheduling result for a task")
    public SchedulingResultEvent findLastSchedulingResult(@PathVariable("id") String taskId) {
        return Responses.fromMono(schedulerService.findLastSchedulingResult(taskId));
    }
}
