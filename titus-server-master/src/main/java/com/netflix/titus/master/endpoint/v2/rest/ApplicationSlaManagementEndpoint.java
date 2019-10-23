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

import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;

/**
 * REST endpoint for application SLA management. See {@link ApplicationSlaRepresentation} for more information.
 * <p>
 * <h1>Application with no associated SLA</h1>
 * If an application has no associated SLA, created via this API, it will be assigned to the default group.
 * This group is associated with a virtual 'default' application. The 'default' application's SLA can be managed
 * as any other via this REST API.
 * <p>
 * <h1>Persistence guarantees</h1>
 */
@Path(ApplicationSlaManagementEndpoint.PATH_API_V2_MANAGEMENT_APPLICATIONS)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface ApplicationSlaManagementEndpoint {

    String PATH_API_V2_MANAGEMENT_APPLICATIONS = "/api/v2/management/applications";

    String DEFAULT_APPLICATION = "DEFAULT";

    /**
     * Returns all registered application SLAs.
     *
     * @return a collection of application SLAs or empty array if non present
     */
    @GET
    List<ApplicationSlaRepresentation> getApplicationSLAs(@QueryParam("extended") boolean extended);

    /**
     * Returns application SLA data for a given application.
     *
     * @return HTTP 200, and application SLA in the response body or HTTP 404 if application SLA is not defined
     * for the given application
     */
    @GET
    @Path("/{applicationName}")
    ApplicationSlaRepresentation getApplicationSLA(@PathParam("applicationName") String applicationName,
                                                   @QueryParam("extended") boolean extended);

    /**
     * Adds a new SLA for an application. If SLA for the given application was already defined, it is overridden.
     * <p>
     * <h1>Input validation rules</h1>
     * <ul>
     * <li>an instance type is available for service jobs (for example must be m4.2xlarge only)</li>
     * <li>an instance count is within a configured limits: COUNT > 0 && COUNT < MAX_ALLOWED</li>
     * </ul>
     *
     * @return HTTP 204, and location to the newly created resource in 'Location' HTTP header.
     */
    @POST
    Response addApplicationSLA(ApplicationSlaRepresentation applicationSLA);

    /**
     * Updates SLA for a given application. If SLA for the given application is not defined, returns 404.
     *
     * @return HTTP 204, and location to the update resource in 'Location' HTTP header.
     */
    @PUT
    @Path("/{applicationName}")
    Response updateApplicationSLA(@PathParam("applicationName") String applicationName, ApplicationSlaRepresentation applicationSLA);

    /**
     * Removes existing application SLA.
     *
     * @return HTTP 200 or 404 if SLA for the given application was not defined
     */
    @DELETE
    @Path("/{applicationName}")
    Response removeApplicationSLA(@PathParam("applicationName") String applicationName);
}
