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
import com.netflix.titus.api.model.Reference;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.SystemDisruptionBudget;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import rx.Observable;

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

    @ApiOperation("Return the global disruption budget")
    @Path("budgets/global")
    @GET
    public SystemDisruptionBudget getGlobalDisruptionBudget() {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getDisruptionBudget(Reference.global())
                        .map(GrpcEvictionModelConverters::toGrpcSystemDisruptionBudget)
        );
    }

    @ApiOperation("Return a tier disruption budget")
    @Path("budgets/tiers/{tier}")
    @GET
    public SystemDisruptionBudget getTierDisruptionBudget(@PathParam("tier") String tier) {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getDisruptionBudget(Reference.tier(StringExt.parseEnumIgnoreCase(tier, Tier.class)))
                        .map(GrpcEvictionModelConverters::toGrpcSystemDisruptionBudget)
        );
    }

    @ApiOperation("Return a capacity group disruption budget")
    @Path("budgets/capacityGroups/{name}")
    @GET
    public Observable<SystemDisruptionBudget> getCapacityGroupDisruptionBudget(@PathParam("name") String capacityGroupName) {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getDisruptionBudget(Reference.capacityGroup(capacityGroupName))
                        .map(GrpcEvictionModelConverters::toGrpcSystemDisruptionBudget)
        );
    }

    @ApiOperation("Return the global eviction quota")
    @Path("quotas/global")
    @GET
    public EvictionQuota getGlobalEvictionQuota() {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getEvictionQuota(Reference.global())
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Return a tier eviction quota")
    @Path("quotas/tiers/{tier}")
    @GET
    public EvictionQuota getTierEvictionQuota(@PathParam("tier") String tier) {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getEvictionQuota(Reference.tier(StringExt.parseEnumIgnoreCase(tier, Tier.class)))
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Return a capacity group eviction quota")
    @Path("quotas/capacityGroups/{name}")
    @GET
    public EvictionQuota getCapacityGroupEvictionQuota(@PathParam("name") String capacityGroupName) {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .getEvictionQuota(Reference.capacityGroup(capacityGroupName))
                        .map(GrpcEvictionModelConverters::toGrpcEvictionQuota)
        );
    }

    @ApiOperation("Terminate a task using the eviction service")
    @Path("tasks/{id}")
    @DELETE
    public TaskTerminateResponse terminateTask(@PathParam("id") String taskId,
                                               @QueryParam("reason") String reason) {
        return Responses.fromSingleValueObservable(
                evictionServiceClient
                        .terminateTask(taskId, reason)
                        .toObservable()
                        .materialize()
                        .flatMap(event -> {
                            switch (event.getKind()) {
                                case OnError:
                                    if (event.getThrowable() instanceof EvictionException) {
                                        return Observable.just(TaskTerminateResponse.newBuilder()
                                                .setAllowed(false)
                                                .setReasonCode("failure")
                                                .setReasonMessage(event.getThrowable().getMessage())
                                                .build()
                                        );
                                    }
                                    return Observable.error(event.getThrowable());
                                case OnCompleted:
                                    return Observable.just(TaskTerminateResponse.newBuilder()
                                            .setAllowed(true)
                                            .build()
                                    );
                            }
                            return Observable.error(new IllegalStateException("Unexpected event kind: " + event.getKind()));
                        })
        );
    }
}
