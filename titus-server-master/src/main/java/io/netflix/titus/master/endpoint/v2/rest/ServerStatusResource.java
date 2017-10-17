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


import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.netflix.titus.api.endpoint.v2.rest.representation.ServerStatusRepresentation;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.common.util.guice.ActivationLifecycle;
import io.netflix.titus.master.cluster.LeaderActivator;

/**
 * Provides local server status information.
 */
@Path(ServerStatusResource.PATH_API_V2_STATUS)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ServerStatusResource {

    public static final String PATH_API_V2_STATUS = "/api/v2/status";

    private final ActivationLifecycle activationLifecycle;
    private final LeaderActivator leaderActivator;

    @Inject
    public ServerStatusResource(ActivationLifecycle activationLifecycle,
                                LeaderActivator leaderActivator) {
        this.activationLifecycle = activationLifecycle;
        this.leaderActivator = leaderActivator;
    }

    @GET
    public ServerStatusRepresentation getServerStatus() {
        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        long uptime = rb.getUptime();

        if (!leaderActivator.isLeader()) {
            return new ServerStatusRepresentation(
                    DateTimeExt.toUtcDateTimeString(uptime),
                    false,
                    null,
                    null,
                    null,
                    null
            );
        }

        List<ServerStatusRepresentation.ServiceActivation> serviceActivations = activationLifecycle.getServiceActionTimesMs().stream()
                .map(p -> new ServerStatusRepresentation.ServiceActivation(p.getLeft(), DateTimeExt.toTimeUnitString(p.getRight())))
                .collect(Collectors.toList());

        return new ServerStatusRepresentation(
                DateTimeExt.toTimeUnitString(uptime),
                true,
                DateTimeExt.toUtcDateTimeString(leaderActivator.getElectionTime()),
                DateTimeExt.toUtcDateTimeString(leaderActivator.getActivationTime()),
                DateTimeExt.toTimeUnitString(activationLifecycle.getActivationTimeMs()),
                serviceActivations
        );
    }
}
