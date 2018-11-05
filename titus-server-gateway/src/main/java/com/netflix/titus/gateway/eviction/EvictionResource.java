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

package com.netflix.titus.gateway.eviction;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Mono;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Eviction service")
@Path("/v3/eviction")
@Singleton
public class EvictionResource {

    private final EvictionServiceClient evictionServiceClient;

    @Inject
    public EvictionResource(EvictionServiceClient evictionServiceClient) {
        this.evictionServiceClient = evictionServiceClient;
    }

    @ApiOperation("Return the system eviction quota")
    @Path("quotas/system")
    @GET
    public EvictionQuota getSystemEvictionQuota() {
        return Responses.fromMono(
                evictionServiceClient
                        .getEvictionQuota(Reference.system())
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Return a tier eviction quota")
    @Path("quotas/tiers/{tier}")
    @GET
    public EvictionQuota getTierEvictionQuota(@PathParam("tier") String tier) {
        return Responses.fromMono(
                evictionServiceClient
                        .getEvictionQuota(Reference.tier(StringExt.parseEnumIgnoreCase(tier, Tier.class)))
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Return a capacity group eviction quota")
    @Path("quotas/capacityGroups/{name}")
    @GET
    public EvictionQuota getCapacityGroupEvictionQuota(@PathParam("name") String capacityGroupName) {
        return Responses.fromMono(
                evictionServiceClient
                        .getEvictionQuota(Reference.capacityGroup(capacityGroupName))
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Return a job eviction quota")
    @Path("quotas/jobs/{id}")
    @GET
    public EvictionQuota getJobEvictionQuota(@PathParam("id") String jobId) {
        return Responses.fromMono(
                evictionServiceClient
                        .getEvictionQuota(Reference.job(jobId))
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Terminate a task using the eviction service")
    @Path("tasks/{id}")
    @DELETE
    public TaskTerminateResponse terminateTask(@PathParam("id") String taskId,
                                               @QueryParam("reason") String reason) {
        return Responses.fromMono(
                evictionServiceClient
                        .terminateTask(taskId, reason)
                        .materialize()
                        .flatMap(event -> {
                            switch (event.getType()) {
                                case ON_ERROR:
                                    if (event.getThrowable() instanceof EvictionException) {
                                        return Mono.just(TaskTerminateResponse.newBuilder()
                                                .setAllowed(false)
                                                .setReasonCode("failure")
                                                .setReasonMessage(event.getThrowable().getMessage())
                                                .build()
                                        );
                                    }
                                    return Mono.error(event.getThrowable());
                                case ON_COMPLETE:
                                    return Mono.just(TaskTerminateResponse.newBuilder()
                                            .setAllowed(true)
                                            .setReasonCode("normal")
                                            .setReasonMessage("Terminated")
                                            .build()
                                    );
                            }
                            return Mono.empty();
                        })
        );
    }
}
