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

package com.netflix.titus.supplementary.relocation.endpoint.rest;

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.endpoint.representation.EvictionRepresentations;
import com.netflix.titus.common.framework.scheduler.endpoint.representation.ScheduleRepresentation;
import com.netflix.titus.common.runtime.TitusRuntime;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO Move to the titus-common package
 */
@RestController
@RequestMapping(path = "/api/diagnostic/localScheduler")
public class LocalSchedulerSpringResource {

    private final LocalScheduler localScheduler;

    @Inject
    public LocalSchedulerSpringResource(TitusRuntime titusRuntime) {
        this.localScheduler = titusRuntime.getLocalScheduler();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/schedules", produces = "application/json")
    public List<ScheduleRepresentation> getActiveSchedules() {
        return localScheduler.getActiveSchedules().stream().map(EvictionRepresentations::toRepresentation).collect(Collectors.toList());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/schedules/{name}", produces = "application/json")
    public ScheduleRepresentation getActiveSchedule(@PathVariable("name") String name) {
        return localScheduler.getActiveSchedules().stream()
                .filter(s -> s.getDescriptor().getName().equals(name))
                .findFirst()
                .map(EvictionRepresentations::toRepresentation)
                .orElseThrow(() -> new WebApplicationException(Response.status(404).build()));
    }

    @RequestMapping(method = RequestMethod.GET, path = "/archived", produces = "application/json")
    public List<ScheduleRepresentation> getArchivedSchedules() {
        return localScheduler.getArchivedSchedules().stream()
                .map(EvictionRepresentations::toRepresentation)
                .collect(Collectors.toList());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/archived/{name}", produces = "application/json")
    public ScheduleRepresentation getArchivedSchedule(@PathVariable("name") String name) {
        return localScheduler.getArchivedSchedules().stream()
                .filter(s -> s.getDescriptor().getName().equals(name))
                .findFirst()
                .map(EvictionRepresentations::toRepresentation)
                .orElseThrow(() -> new WebApplicationException(Response.status(404).build()));
    }
}
