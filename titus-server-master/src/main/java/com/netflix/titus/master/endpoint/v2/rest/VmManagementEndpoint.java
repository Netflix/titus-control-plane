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

import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.scheduler.VMOperations;

/**
 * REST API endpoint for Titus agent VM management.
 */
@Path(VmManagementEndpoint.PATH_API_V2_VM)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface VmManagementEndpoint {

    String PATH_API_V2_VM = "/api/v2/vm";

    String PATH_LIST_SERVER_INFOS = "/activevms/listserverinfos";
    String PATH_LIST_AGENTS = "/activevms/listagents";
    String PATH_LIST_JOBS_ON_VM = "/activevms/listjobsonvms";

    @GET
    @Path(PATH_LIST_SERVER_INFOS)
    List<ServerInfo> getServerInfos();

    @GET
    @Path(PATH_LIST_AGENTS)
    List<VMOperations.AgentInfo> getAgents(@QueryParam("detached") Boolean detached);

    @GET
    @Path(PATH_LIST_JOBS_ON_VM)
    Map<String, List<VMOperations.JobsOnVMStatus>> getJobsOnVM(
            @QueryParam("idleOnly") boolean idleOnly,
            @QueryParam("excludeIdle") boolean excludeIdle
    );
}
