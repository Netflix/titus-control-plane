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

package com.netflix.titus.gateway.endpoint.v3.rest;


import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.gateway.service.v3.AutoScalingService;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Auto scaling")
@Path("/v3/autoscaling")
@Singleton
public class AutoScalingResource {
    private static Logger log = LoggerFactory.getLogger(AutoScalingResource.class);
    private AutoScalingService autoScalingService;

    @Inject
    public AutoScalingResource(AutoScalingService autoScalingService) {
        this.autoScalingService = autoScalingService;
    }

    @GET
    @ApiOperation("Find scaling policies for a job")
    @Path("scalingPolicies")
    public GetPolicyResult getAllScalingPolicies() {
        GetPolicyResult getPolicyResult = Responses.fromSingleValueObservable(autoScalingService.getAllScalingPolicies());
        return getPolicyResult;
    }

    @GET
    @ApiOperation("Find scaling policies for a job")
    @Path("scalingPolicies/{jobId}")
    public GetPolicyResult getScalingPolicyForJob(@PathParam("jobId") String jobId) {
        JobId request = JobId.newBuilder().setId(jobId).build();
        GetPolicyResult getPolicyResult = Responses.fromSingleValueObservable(autoScalingService.getJobScalingPolicies(request));
        return getPolicyResult;
    }

    @POST
    @ApiOperation("Create or Update scaling policy")
    @Path("scalingPolicy")
    public ScalingPolicyID setScalingPolicy(PutPolicyRequest putPolicyRequest) {
        Observable<ScalingPolicyID> putPolicyResult = autoScalingService.setAutoScalingPolicy(putPolicyRequest);
        ScalingPolicyID policyId = Responses.fromSingleValueObservable(putPolicyResult);
        log.info("New policy created {}", policyId);
        return policyId;
    }


    @GET
    @ApiOperation("Find scaling policy for a policy Id")
    @Path("scalingPolicy/{policyId}")
    public GetPolicyResult getScalingPolicy(@PathParam("policyId") String policyId) {
        ScalingPolicyID scalingPolicyId = ScalingPolicyID.newBuilder().setId(policyId).build();
        return Responses.fromSingleValueObservable(autoScalingService.getScalingPolicy(scalingPolicyId));
    }

    @DELETE
    @ApiOperation("Delete scaling policy")
    @Path("scalingPolicy/{policyId}")
    public javax.ws.rs.core.Response removePolicy(@PathParam("policyId") String policyId) {
        ScalingPolicyID scalingPolicyId = ScalingPolicyID.newBuilder().setId(policyId).build();
        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder().setId(scalingPolicyId).build();
        return Responses.fromCompletable(autoScalingService.deleteAutoScalingPolicy(deletePolicyRequest));
    }


    @PUT
    @ApiOperation("Update scaling policy")
    @Path("scalingPolicy")
    public javax.ws.rs.core.Response updateScalingPolicy(UpdatePolicyRequest updatePolicyRequest) {
        return Responses.fromCompletable(autoScalingService.updateAutoScalingPolicy(updatePolicyRequest));
    }
}
