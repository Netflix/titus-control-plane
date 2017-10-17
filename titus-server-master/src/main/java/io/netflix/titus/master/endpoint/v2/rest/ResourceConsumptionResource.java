/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.netflix.titus.master.service.management.CompositeResourceConsumption;
import io.netflix.titus.master.service.management.ResourceConsumptionService;

@Path(ResourceConsumptionEndpoint.PATH_API_V2_RESOURCE_CONSUMPTION)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ResourceConsumptionResource implements ResourceConsumptionEndpoint {

    private final ResourceConsumptionService service;

    @Inject
    public ResourceConsumptionResource(ResourceConsumptionService service) {
        this.service = service;
    }

    @Override
    public Response getSystemConsumption() {
        Optional<CompositeResourceConsumption> systemConsumption = service.getSystemConsumption();
        if (systemConsumption.isPresent()) {
            return Response.ok(systemConsumption.get()).build();
        }
        return Response.status(Status.SERVICE_UNAVAILABLE).build();
    }
}
