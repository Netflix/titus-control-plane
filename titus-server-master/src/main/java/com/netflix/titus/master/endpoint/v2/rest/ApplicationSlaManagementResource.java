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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.endpoint.v2.rest.representation.ReservationUsage;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.swagger.annotations.Api;

import static com.netflix.titus.master.endpoint.v2.rest.Representation2ModelConvertions.asCoreEntity;
import static com.netflix.titus.master.endpoint.v2.rest.Representation2ModelConvertions.asRepresentation;

@Api(tags = "SLA")
@Path(ApplicationSlaManagementEndpoint.PATH_API_V2_MANAGEMENT_APPLICATIONS)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ApplicationSlaManagementResource implements ApplicationSlaManagementEndpoint {

    private final MasterConfiguration configuration;
    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final ReservationUsageCalculator reservationUsageCalculator;

    @Context
    private HttpServletRequest httpServletRequest;

    @Inject
    public ApplicationSlaManagementResource(MasterConfiguration configuration,
                                            ApplicationSlaManagementService applicationSlaManagementService,
                                            ReservationUsageCalculator reservationUsageCalculator) {
        this.configuration = configuration;
        this.applicationSlaManagementService = applicationSlaManagementService;
        this.reservationUsageCalculator = reservationUsageCalculator;
    }

    @GET
    @Override
    public List<ApplicationSlaRepresentation> getApplicationSLAs(@QueryParam("extended") boolean extended) {
        List<ApplicationSlaRepresentation> result = new ArrayList<>();

        String cellId = extended ? configuration.getCellName() : null;
        Map<String, ReservationUsage> usageMap = extended ? reservationUsageCalculator.buildUsage() : Collections.emptyMap();

        applicationSlaManagementService.getApplicationSLAs().forEach(a -> result.add(asRepresentation(a, cellId, usageMap.get(a.getAppName()))));
        return result;
    }

    @GET
    @Path("/{applicationName}")
    @Override
    public ApplicationSlaRepresentation getApplicationSLA(@PathParam("applicationName") String applicationName,
                                                          @QueryParam("extended") boolean extended) {
        ApplicationSLA applicationSLA = applicationSlaManagementService.getApplicationSLA(applicationName);
        if (applicationSLA == null) {
            throw new WebApplicationException(new IllegalArgumentException("SLA not defined for " + applicationName), Status.NOT_FOUND);
        }

        String cellId = extended ? configuration.getCellName() : null;
        ReservationUsage reservationUsage = extended ? reservationUsageCalculator.buildCapacityGroupUsage(applicationName) : null;

        return asRepresentation(applicationSLA, cellId, reservationUsage);
    }

    @POST
    @Override
    public Response addApplicationSLA(ApplicationSlaRepresentation applicationSLA) {
        ApplicationSLA existing = applicationSlaManagementService.getApplicationSLA(applicationSLA.getAppName());
        if (existing != null) {
            throw new WebApplicationException(
                    new IllegalStateException("Application SLA for " + applicationSLA.getAppName() + " already exist"),
                    Status.CONFLICT
            );
        }
        applicationSlaManagementService.addApplicationSLA(asCoreEntity(applicationSLA)).timeout(1, TimeUnit.MINUTES).toBlocking().firstOrDefault(null);
        return Response.created(URI.create(applicationSLA.getAppName())).build();
    }

    @PUT
    @Path("/{applicationName}")
    @Override
    public Response updateApplicationSLA(@PathParam("applicationName") String applicationName,
                                         ApplicationSlaRepresentation applicationSLA) {
        if (!applicationName.equals(applicationSLA.getAppName())) {
            throw new IllegalArgumentException("application name in path different from appName in the request body");
        }
        ApplicationSLA existing = applicationSlaManagementService.getApplicationSLA(applicationSLA.getAppName());
        if (existing == null) {
            throw new WebApplicationException(new IllegalArgumentException("SLA not defined for " + applicationName), Status.NOT_FOUND);
        }

        applicationSlaManagementService.addApplicationSLA(asCoreEntity(applicationSLA)).timeout(1, TimeUnit.MINUTES).toBlocking().firstOrDefault(null);
        return Response.status(Status.NO_CONTENT).build();
    }

    @DELETE
    @Path("/{applicationName}")
    @Override
    public Response removeApplicationSLA(@PathParam("applicationName") String applicationName) {
        if (DEFAULT_APPLICATION.equals(applicationName)) {
            throw new WebApplicationException(new IllegalArgumentException("DEFAULT application SLA cannot be removed"), Status.BAD_REQUEST);
        }
        ApplicationSLA existing = applicationSlaManagementService.getApplicationSLA(applicationName);
        if (existing == null) {
            throw new WebApplicationException(new IllegalArgumentException("SLA not defined for " + applicationName), Status.NOT_FOUND);
        }

        applicationSlaManagementService.removeApplicationSLA(applicationName).timeout(1, TimeUnit.MINUTES).toBlocking().firstOrDefault(null);
        return Response.status(Status.NO_CONTENT).location(URI.create(PATH_API_V2_MANAGEMENT_APPLICATIONS + '/' + applicationName)).build();
    }
}
