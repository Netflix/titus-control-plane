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

import javax.inject.Inject;

import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.service.HealthService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Health")
@RestController
@RequestMapping(path = "/api/v3/status", produces = "application/json")
public class HealthSpringResource {

    private final HealthService healthService;

    @Inject
    public HealthSpringResource(HealthService healthService) {
        this.healthService = healthService;
    }

    @ApiOperation("Get the health status")
    @GetMapping
    public HealthCheckResponse check() {
        return Responses.fromSingleValueObservable(healthService.check(HealthCheckRequest.newBuilder().build()));
    }

}
