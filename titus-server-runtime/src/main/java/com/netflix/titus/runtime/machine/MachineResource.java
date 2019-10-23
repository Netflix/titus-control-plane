/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.machine;

import java.time.Duration;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.runtime.connector.machine.ReactorMachineServiceStub;
import com.netflix.titus.runtime.endpoint.v3.rest.RestUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Machine resources")
@Path("/v4")
@Singleton
public class MachineResource {

    // We depend on client timeouts, which are dynamically configurable. This is just reasonable upper bound if
    // they do not trigger.
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final ReactorMachineServiceStub machineServiceStub;

    @Inject
    public MachineResource(ReactorMachineServiceStub machineServiceStub) {
        this.machineServiceStub = machineServiceStub;
    }

    @GET
    @ApiOperation("Get all machines")
    @Path("/machines")
    public MachineQueryResult getMachines(@Context UriInfo info) {
        MultivaluedMap<String, String> queryParameters = info.getQueryParameters(true);

        QueryRequest.Builder queryBuilder = QueryRequest.newBuilder();
        Page page = RestUtil.createPage(queryParameters);

        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));

        return machineServiceStub.getMachines(queryBuilder.build()).block(TIMEOUT);
    }

    @GET
    @ApiOperation("Get single machine")
    @Path("/machines/{machineId}")
    public Machine getMachine(@PathParam("machineId") String machineId) {
        return machineServiceStub.getMachine(Id.newBuilder().setId(machineId).build()).block(TIMEOUT);
    }
}
