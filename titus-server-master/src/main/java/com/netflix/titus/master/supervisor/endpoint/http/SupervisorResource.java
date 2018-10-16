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

package com.netflix.titus.master.supervisor.endpoint.http;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters;
import com.netflix.titus.master.supervisor.service.SupervisorOperations;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Titus Supervisor")
@Path("/api/v3/supervisor")
@Singleton
public class SupervisorResource {

    private final SupervisorOperations supervisorOperations;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public SupervisorResource(SupervisorOperations supervisorOperations,
                              CallMetadataResolver callMetadataResolver) {
        this.supervisorOperations = supervisorOperations;
        this.callMetadataResolver = callMetadataResolver;
    }

    @GET
    @ApiOperation("Get all TitusMaster instances")
    @Path("/instances")
    public List<MasterInstance> getAllMasterInstances() {
        return supervisorOperations.getMasterInstances().stream()
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .collect(Collectors.toList());
    }

    @GET
    @ApiOperation("Find the TitusMaster instance with the specified id")
    @Path("/instances/{masterInstanceId}")
    public MasterInstance getMasterInstance(@PathParam("masterInstanceId") String masterInstanceId) {
        return supervisorOperations.findMasterInstance(masterInstanceId)
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .orElseThrow(() -> new WebApplicationException(Response.Status.NOT_FOUND));
    }

    @GET
    @ApiOperation("Return current leader")
    @Path("/instances/leader")
    public MasterInstance getLeader() {
        return supervisorOperations.findLeader()
                .map(SupervisorGrpcModelConverters::toGrpcMasterInstance)
                .orElseThrow(() -> new WebApplicationException(Response.Status.NOT_FOUND));
    }

    @DELETE
    @ApiOperation("If leader, give up the leadership")
    @Path("/self/stopBeingLeader")
    public Response stopBeingLeader() {
        Optional<CallMetadata> callMetadataOpt = callMetadataResolver.resolve();
        boolean validUser = callMetadataOpt.map(cm -> !CallMetadataUtils.isUnknown(cm)).orElse(false);
        if (!validUser) {
            return Response.status(Response.Status.FORBIDDEN).entity("Unidentified request").build();
        }
        supervisorOperations.stopBeingLeader(callMetadataOpt.get());
        return Response.ok().build();
    }
}
