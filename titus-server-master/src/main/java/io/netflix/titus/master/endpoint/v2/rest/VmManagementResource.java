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

import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.scheduler.VMOperations;
import io.swagger.annotations.Api;

@Api(tags = "VM")
@Path(VmManagementEndpoint.PATH_API_V2_VM)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class VmManagementResource implements VmManagementEndpoint {

    private final ServerInfoResolver serverInfoResolver;
    private final VMOperations vmOps;

    @Inject
    public VmManagementResource(ServerInfoResolver serverInfoResolver, VMOperations vmOps) {
        this.serverInfoResolver = serverInfoResolver;
        this.vmOps = vmOps;
    }

    @GET
    @Path(PATH_LIST_SERVER_INFOS)
    @Override
    public List<ServerInfo> getServerInfos() {
        return serverInfoResolver.resolveAll();
    }

    @GET
    @Path(PATH_LIST_AGENTS)
    @Override
    public List<VMOperations.AgentInfo> getAgents(@QueryParam("detached") Boolean detached) {
        return Boolean.TRUE.equals(detached) ? vmOps.getDetachedAgentInfos() : vmOps.getAgentInfos();
    }

    @GET
    @Path(PATH_LIST_JOBS_ON_VM)
    @Override
    public Map<String, List<VMOperations.JobsOnVMStatus>> getJobsOnVM(
            @QueryParam("idleOnly") boolean idleOnly,
            @QueryParam("excludeIdle") boolean excludeIdle) {
        return vmOps.getJobsOnVMs(idleOnly, excludeIdle);
    }
}
