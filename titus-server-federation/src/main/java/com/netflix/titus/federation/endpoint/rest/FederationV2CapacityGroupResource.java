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

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.federation.endpoint.EndpointConfiguration;
import com.netflix.titus.federation.service.CellWebClientConnector;
import com.netflix.titus.federation.service.CellWebClientConnectorUtil;
import io.swagger.annotations.Api;
import org.springframework.core.ParameterizedTypeReference;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "CapacityManagement")
@Path(FederationV2CapacityGroupResource.BASE_PATH)
@Singleton
public class FederationV2CapacityGroupResource {

    public static final String BASE_PATH = "/v2/management/applications";

    public static final String API_PATH = "/api/v2/management/applications";

    private static final ParameterizedTypeReference<ApplicationSlaRepresentation> APPLICATION_SLA_TP =
            ParameterizedTypeReference.forType(ApplicationSlaRepresentation.class);

    private static final ParameterizedTypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_LIST_TP =
            new ParameterizedTypeReference<List<ApplicationSlaRepresentation>>() {
            };

    private final EndpointConfiguration configuration;
    private final CellWebClientConnector cellWebClientConnector;

    @Inject
    public FederationV2CapacityGroupResource(EndpointConfiguration configuration,
                                             CellWebClientConnector cellWebClientConnector) {
        this.configuration = configuration;
        this.cellWebClientConnector = cellWebClientConnector;
    }

    /**
     * Returns all registered application SLAs.
     *
     * @return a collection of application SLAs or empty array if non present
     */
    @GET
    public List<ApplicationSlaRepresentation> getApplicationSLAs(@QueryParam("extended") boolean extended) {
        Either<List<ApplicationSlaRepresentation>, WebApplicationException> result = CellWebClientConnectorUtil.doGetAndMerge(
                cellWebClientConnector,
                API_PATH + "?extended=" + extended,
                APPLICATION_SLA_LIST_TP,
                configuration.getRestRequestTimeoutMs()
        );
        if (result.hasError()) {
            throw result.getError();
        }
        return result.getValue();
    }

    /**
     * Returns application SLA data for a given application.
     *
     * @return HTTP 200, and application SLA in the response body or HTTP 404 if application SLA is not defined
     * for the given application
     */
    @GET
    @Path("/{applicationName}")
    public ApplicationSlaRepresentation getApplicationSLA(@PathParam("applicationName") String applicationName,
                                                          @QueryParam("extended") boolean extended) {
        Either<ApplicationSlaRepresentation, WebApplicationException> result = CellWebClientConnectorUtil.doGetFromCell(
                cellWebClientConnector,
                API_PATH
                        + '/' + applicationName + "?extended=" + extended,
                APPLICATION_SLA_TP,
                configuration.getRestRequestTimeoutMs()
        );
        if (result.hasError()) {
            throw result.getError();
        }
        return result.getValue();
    }
}
