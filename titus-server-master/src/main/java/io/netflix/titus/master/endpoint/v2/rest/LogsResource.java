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

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.netflix.titus.api.endpoint.v2.rest.representation.LogLinksRepresentation;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.master.endpoint.v2.V2LegacyTitusServiceGateway;
import io.netflix.titus.runtime.endpoint.common.rest.Responses;

@Path(LogsEndpoint.PATH_API_V2_LOGS)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class LogsResource implements LogsEndpoint {

    private final V2LegacyTitusServiceGateway serviceGateway;

    @Inject
    public LogsResource(V2LegacyTitusServiceGateway serviceGateway) {
        this.serviceGateway = serviceGateway;
    }

    @GET
    @Path("/{taskId}")
    @Override
    public LogLinksRepresentation getLogs(@PathParam("taskId") String taskId) {
        TitusTaskInfo taskInfo = Responses.fromSingleValueObservable(serviceGateway.findTaskById(taskId));
        return new LogLinksRepresentation(taskInfo.getStdoutLive(), taskInfo.getLogs(), taskInfo.getSnapshots());
    }
}
