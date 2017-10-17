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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.netflix.titus.api.endpoint.v2.rest.representation.LogLinksRepresentation;

/**
 */
@Path(LogsEndpoint.PATH_API_V2_LOGS)
@Produces(MediaType.APPLICATION_JSON)
public interface LogsEndpoint {
    String PATH_API_V2_LOGS = "/api/v2/logs";

    @GET
    @Path("{taskId}")
    LogLinksRepresentation getLogs(@PathParam("taskId") String taskId);
}
