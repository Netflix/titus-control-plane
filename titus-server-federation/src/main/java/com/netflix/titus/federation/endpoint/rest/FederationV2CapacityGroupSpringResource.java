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

import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.federation.endpoint.EndpointConfiguration;
import com.netflix.titus.federation.service.CellWebClientConnector;
import com.netflix.titus.federation.service.CellWebClientConnectorUtil;
import io.swagger.annotations.Api;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "CapacityManagement")
@RestController
@RequestMapping(path = "/api/v2/management/applications", produces = "application/json")
public class FederationV2CapacityGroupSpringResource {

    public static final String API_PATH = "/api/v2/management/applications";

    private static final ParameterizedTypeReference<ApplicationSlaRepresentation> APPLICATION_SLA_TP =
            ParameterizedTypeReference.forType(ApplicationSlaRepresentation.class);

    private static final ParameterizedTypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_LIST_TP =
            new ParameterizedTypeReference<List<ApplicationSlaRepresentation>>() {
            };

    private final EndpointConfiguration configuration;
    private final CellWebClientConnector cellWebClientConnector;

    @Inject
    public FederationV2CapacityGroupSpringResource(EndpointConfiguration configuration,
                                                   CellWebClientConnector cellWebClientConnector) {
        this.configuration = configuration;
        this.cellWebClientConnector = cellWebClientConnector;
    }

    /**
     * Returns all registered application SLAs.
     *
     * @return a collection of application SLAs or empty array if non present
     */
    @GetMapping
    public List<ApplicationSlaRepresentation> getApplicationSLAs(@RequestParam(name = "extended", defaultValue = "false") boolean extended) {
        Either<List<ApplicationSlaRepresentation>, WebApplicationException> result = CellWebClientConnectorUtil.doGetAndMerge(
                cellWebClientConnector,
                API_PATH + "?extended=" + extended,
                APPLICATION_SLA_LIST_TP,
                configuration.getRestRequestTimeoutMs()
        );
        return result.must();
    }

    /**
     * Returns application SLA data for a given application.
     *
     * @return HTTP 200, and application SLA in the response body or HTTP 404 if application SLA is not defined
     * for the given application
     */
    @GetMapping(path = "/{applicationName}")
    public ApplicationSlaRepresentation getApplicationSLA(@PathVariable("applicationName") String applicationName,
                                                          @RequestParam(name = "extended", defaultValue = "false") boolean extended) {
        Either<ApplicationSlaRepresentation, WebApplicationException> result = CellWebClientConnectorUtil.doGetFromCell(
                cellWebClientConnector,
                API_PATH
                        + '/' + applicationName + "?extended=" + extended,
                APPLICATION_SLA_TP,
                configuration.getRestRequestTimeoutMs()
        );
        return result.must();
    }
}
