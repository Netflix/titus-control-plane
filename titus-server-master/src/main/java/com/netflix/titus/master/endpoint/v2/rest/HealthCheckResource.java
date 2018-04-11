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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.runtime.health.api.HealthCheckStatus;

@Path("/health")
@Singleton
public class HealthCheckResource {

    private final HealthCheckAggregator healthCheck;

    @Inject
    public HealthCheckResource(HealthCheckAggregator healthCheck) {
        this.healthCheck = healthCheck;
    }

    @GET
    public Response doCheck() throws Exception {
        HealthCheckStatus healthCheckStatus = healthCheck.check().get(2, TimeUnit.SECONDS);
        if (healthCheckStatus.isHealthy()) {
            return Response.ok(healthCheckStatus).build();
        } else {
            return Response.serverError().entity(healthCheckStatus).build();
        }
    }
}
