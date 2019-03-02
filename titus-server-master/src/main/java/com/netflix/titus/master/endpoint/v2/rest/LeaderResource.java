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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.endpoint.v2.rest.representation.LeaderRepresentation;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import com.netflix.titus.master.mesos.MesosMasterResolver;

/**
 */
@Path(LeaderEndpoint.PATH_API_V2_LEADER)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class LeaderResource implements LeaderEndpoint {

    private final MesosMasterResolver mesosMasterResolver;
    private final MasterMonitor masterMonitor;

    @Inject
    public LeaderResource(MasterMonitor masterMonitor, MesosMasterResolver mesosMasterResolver) {
        this.masterMonitor = masterMonitor;
        this.mesosMasterResolver = mesosMasterResolver;
    }

    @GET
    public LeaderRepresentation getLeader() {
        String mesosLeader = mesosMasterResolver.resolveLeader().map(LeaderResource::inetToString).orElse(null);
        List<String> mesosAddresses = mesosMasterResolver.resolveMesosAddresses().stream()
                .map(LeaderResource::inetToString)
                .collect(Collectors.toList());

        MasterDescription masterDescription = masterMonitor.getLatestLeader();

        LeaderRepresentation.Builder builder = LeaderRepresentation.newBuilder()
                .withHostname(masterDescription.getHostname())
                .withHostIP(masterDescription.getHostIP())
                .withApiPort(masterDescription.getApiPort())
                .withApiStatusUri(masterDescription.getApiStatusUri())
                .withCreateTime(masterDescription.getCreateTime())
                .withMesosLeader(mesosLeader)
                .withMesosServers(mesosAddresses);

        return builder.build();
    }

    private static String inetToString(InetSocketAddress a) {
        return a.getHostString() + ':' + a.getPort();
    }
}
