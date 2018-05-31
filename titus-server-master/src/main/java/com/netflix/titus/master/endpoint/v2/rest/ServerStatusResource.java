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


import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.netflix.titus.api.endpoint.v2.rest.representation.ServerStatusRepresentation;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.Details;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus;
import com.netflix.titus.grpc.protogen.ServiceActivation;
import com.netflix.titus.master.health.service.HealthService;

/**
 * Provides local server status information.
 */
@Path(ServerStatusResource.PATH_API_V2_STATUS)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ServerStatusResource {

    public static final String PATH_API_V2_STATUS = "/api/v2/status";
    public static final String NOT_APPLICABLE = "N/A";

    private final HealthService healthService;

    @Inject
    public ServerStatusResource(HealthService healthService) {
        this.healthService = healthService;
    }

    @GET
    public ServerStatusRepresentation getServerStatus() {
        Details details = healthService.getServerStatus();

        if (details.getStatus() != ServingStatus.SERVING) {
            return new ServerStatusRepresentation(
                    false,
                    false,
                    DateTimeExt.toTimeUnitString(details.getUptime()),
                    NOT_APPLICABLE,
                    NOT_APPLICABLE,
                    NOT_APPLICABLE,
                    Collections.emptyList(),
                    Collections.emptyList()
            );
        }

        List<ServerStatusRepresentation.ServiceActivation> sortedByActivationTime = details.getServiceActivationTimesList().stream()
                .sorted(Comparator.comparing(ServiceActivation::getActivationTime, Durations.comparator()))
                .map(s -> new ServerStatusRepresentation.ServiceActivation(s.getName(), DateTimeExt.toTimeUnitString(s.getActivationTime())))
                .collect(Collectors.toList());

        List<String> namesSortedByActivationTimestamp = details.getServiceActivationTimesList().stream()
                .map(ServiceActivation::getName)
                .collect(Collectors.toList());

        return new ServerStatusRepresentation(
                details.getLeader(),
                details.getActive(),
                DateTimeExt.toTimeUnitString(details.getUptime()),
                Timestamps.toString(details.getElectionTimestamp()),
                Timestamps.toString(details.getActivationTimestamp()),
                details.getActive() ? DateTimeExt.toTimeUnitString(details.getActivationTime()) : NOT_APPLICABLE,
                sortedByActivationTime,
                namesSortedByActivationTimestamp
        );
    }
}
