/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.v3.rest;

import javax.inject.Inject;

import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import com.netflix.titus.runtime.service.AutoScalingService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import rx.Observable;


@Api(tags = "Auto scaling")
@RestController
@RequestMapping(path = "/api/v3/autoscaling", produces = MediaType.APPLICATION_JSON_VALUE)
public class AutoScalingSpringResource {

    private static Logger log = LoggerFactory.getLogger(AutoScalingResource.class);
    private AutoScalingService autoScalingService;

    @Inject
    public AutoScalingSpringResource(AutoScalingService autoScalingService) {
        this.autoScalingService = autoScalingService;
    }

    @ApiOperation("Find scaling policies for a job")
    @GetMapping(path = "/scalingPolicies")
    public GetPolicyResult getAllScalingPolicies(CallMetadataAuthentication authentication) {
        return Responses.fromSingleValueObservable(autoScalingService.getAllScalingPolicies(authentication.getCallMetadata()));
    }

    @ApiOperation("Find scaling policies for a job")
    @GetMapping(path = "/scalingPolicies/{jobId}")
    public GetPolicyResult getScalingPolicyForJob(@PathVariable("jobId") String jobId, CallMetadataAuthentication authentication) {
        JobId request = JobId.newBuilder().setId(jobId).build();
        return Responses.fromSingleValueObservable(autoScalingService.getJobScalingPolicies(request, authentication.getCallMetadata()));
    }

    @ApiOperation("Create or Update scaling policy")
    @PostMapping(path = "/scalingPolicy")
    public ScalingPolicyID setScalingPolicy(@RequestBody PutPolicyRequest putPolicyRequest, CallMetadataAuthentication authentication) {
        Observable<ScalingPolicyID> putPolicyResult = autoScalingService.setAutoScalingPolicy(putPolicyRequest, authentication.getCallMetadata());
        ScalingPolicyID policyId = Responses.fromSingleValueObservable(putPolicyResult);
        log.info("New policy created {}", policyId);
        return policyId;
    }

    @ApiOperation("Find scaling policy for a policy Id")
    @GetMapping(path = "scalingPolicy/{policyId}")
    public GetPolicyResult getScalingPolicy(@PathVariable("policyId") String policyId, CallMetadataAuthentication authentication) {
        ScalingPolicyID scalingPolicyId = ScalingPolicyID.newBuilder().setId(policyId).build();
        return Responses.fromSingleValueObservable(autoScalingService.getScalingPolicy(scalingPolicyId, authentication.getCallMetadata()));
    }

    @ApiOperation("Delete scaling policy")
    @DeleteMapping(path = "scalingPolicy/{policyId}")
    public ResponseEntity<Void> removePolicy(@PathVariable("policyId") String policyId, CallMetadataAuthentication authentication) {
        ScalingPolicyID scalingPolicyId = ScalingPolicyID.newBuilder().setId(policyId).build();
        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder().setId(scalingPolicyId).build();
        return Responses.fromCompletable(autoScalingService.deleteAutoScalingPolicy(deletePolicyRequest, authentication.getCallMetadata()), HttpStatus.OK);
    }

    @ApiOperation("Update scaling policy")
    @PutMapping("scalingPolicy")
    public ResponseEntity<Void> updateScalingPolicy(@RequestBody UpdatePolicyRequest updatePolicyRequest, CallMetadataAuthentication authentication) {
        return Responses.fromCompletable(autoScalingService.updateAutoScalingPolicy(updatePolicyRequest, authentication.getCallMetadata()), HttpStatus.OK);
    }
}
