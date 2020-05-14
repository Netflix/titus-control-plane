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

import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Api(tags = "Load Balancing")
@RestController
@RequestMapping(path = "/api/v3/loadBalancers", produces = MediaType.APPLICATION_JSON_VALUE)
public class LoadBalancerSpringResource {

    private final LoadBalancerService loadBalancerService;
    private final SystemLogService systemLog;

    @Inject
    public LoadBalancerSpringResource(LoadBalancerService loadBalancerService, SystemLogService systemLog) {
        this.loadBalancerService = loadBalancerService;
        this.systemLog = systemLog;
    }

    @ApiOperation("Find the load balancer(s) with the specified ID")
    @GetMapping(path = "/{jobId}")
    public GetJobLoadBalancersResult getJobLoadBalancers(@PathVariable("jobId") String jobId) {
        return Responses.fromSingleValueObservable(loadBalancerService.getLoadBalancers(
                JobId.newBuilder()
                        .setId(jobId)
                        .build()));
    }

    @ApiOperation("Get all load balancers")
    @GetMapping
    public GetAllLoadBalancersResult getAllLoadBalancers(@RequestParam MultiValueMap<String, String> queryParameters,
                                                         CallMetadataAuthentication authentication) {
        Page page = RestUtil.createPage(queryParameters);
        logPageNumberUsage(systemLog, authentication.getCallMetadata(), getClass().getSimpleName(), "getAllLoadBalancers", page);
        return Responses.fromSingleValueObservable(
                loadBalancerService.getAllLoadBalancers(GetAllLoadBalancersRequest.newBuilder()
                        .setPage(page)
                        .build()));
    }

    @ApiOperation("Add a load balancer")
    @PostMapping
    public ResponseEntity<Void> addLoadBalancer(
            @RequestParam("jobId") String jobId,
            @RequestParam("loadBalancerId") String loadBalancerId) {
        return Responses.fromCompletable(
                loadBalancerService.addLoadBalancer(
                        AddLoadBalancerRequest.newBuilder()
                                .setJobId(jobId)
                                .setLoadBalancerId(LoadBalancerId.newBuilder().setId(loadBalancerId).build())
                                .build()
                ),
                HttpStatus.NO_CONTENT
        );
    }

    @ApiOperation("Remove a load balancer")
    @DeleteMapping
    public ResponseEntity<Void> removeLoadBalancer(
            @RequestParam("jobId") String jobId,
            @RequestParam("loadBalancerId") String loadBalancerId) {
        return Responses.fromCompletable(
                loadBalancerService.removeLoadBalancer(
                        RemoveLoadBalancerRequest.newBuilder()
                                .setJobId(jobId)
                                .setLoadBalancerId(LoadBalancerId.newBuilder().setId(loadBalancerId).build())
                                .build()
                ),
                HttpStatus.NO_CONTENT
        );
    }
}
