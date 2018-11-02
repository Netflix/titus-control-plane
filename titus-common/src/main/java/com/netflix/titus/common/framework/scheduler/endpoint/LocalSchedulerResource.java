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

package com.netflix.titus.common.framework.scheduler.endpoint;

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.endpoint.representation.EvictionRepresentations;
import com.netflix.titus.common.framework.scheduler.endpoint.representation.ScheduleRepresentation;
import com.netflix.titus.common.runtime.TitusRuntime;

@Path("/api/diagnostic/localScheduler")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class LocalSchedulerResource {

    private final LocalScheduler localScheduler;

    @Inject
    public LocalSchedulerResource(TitusRuntime titusRuntime) {
        this.localScheduler = titusRuntime.getLocalScheduler();
    }

    @GET
    @Path("/schedules")
    public List<ScheduleRepresentation> getActiveSchedules() {
        return localScheduler.getActiveSchedules().stream().map(EvictionRepresentations::toRepresentation).collect(Collectors.toList());
    }

    @GET
    @Path("/schedules/{name}")
    public ScheduleRepresentation getActiveSchedule(@PathParam("name") String name) {
        return localScheduler.getActiveSchedules().stream()
                .filter(s -> s.getDescriptor().getName().equals(name))
                .findFirst()
                .map(EvictionRepresentations::toRepresentation)
                .orElseThrow(() -> new WebApplicationException(Response.status(404).build()));
    }

    @GET
    @Path("/archived")
    public List<ScheduleRepresentation> getArchivedSchedules() {
        return localScheduler.getArchivedSchedules().stream()
                .map(EvictionRepresentations::toRepresentation)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/archived/{name}")
    public ScheduleRepresentation getArchivedSchedule(@PathParam("name") String name) {
        return localScheduler.getArchivedSchedules().stream()
                .filter(s -> s.getDescriptor().getName().equals(name))
                .findFirst()
                .map(EvictionRepresentations::toRepresentation)
                .orElseThrow(() -> new WebApplicationException(Response.status(404).build()));
    }
}
