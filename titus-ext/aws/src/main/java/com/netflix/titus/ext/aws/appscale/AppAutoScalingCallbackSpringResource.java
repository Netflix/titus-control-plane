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

package com.netflix.titus.ext.aws.appscale;

import javax.inject.Inject;

import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import io.swagger.annotations.Api;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Auto scaling")
@RestController
@RequestMapping(path = "/api/v1/scalableTargetDimensions")
public class AppAutoScalingCallbackSpringResource {

    private final AppAutoScalingCallbackService awsGatewayCallbackService;

    @Inject
    public AppAutoScalingCallbackSpringResource(AppAutoScalingCallbackService awsGatewayCallbackService) {
        this.awsGatewayCallbackService = awsGatewayCallbackService;
    }

    @GetMapping(path = "/{scalableTargetDimensionId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ScalableTargetResourceInfo getScalableTargetResourceInfo(@PathVariable("scalableTargetDimensionId") String jobId,
                                                                    CallMetadataAuthentication authentication) {
        return Responses.fromSingleValueObservable(awsGatewayCallbackService.getScalableTargetResourceInfo(jobId, authentication.getCallMetadata()));
    }

    @PatchMapping(path = "/{scalableTargetDimensionId}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ScalableTargetResourceInfo> setScalableTargetResourceInfo(@PathVariable("scalableTargetDimensionId") String jobId,
                                                                                    @RequestBody ScalableTargetResourceInfo scalableTargetResourceInfo,
                                                                                    CallMetadataAuthentication authentication) {
        if (scalableTargetResourceInfo.getDesiredCapacity() < 0) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        return Responses.fromSingleValueObservable(awsGatewayCallbackService.setScalableTargetResourceInfo(jobId, scalableTargetResourceInfo, authentication.getCallMetadata()));
    }
}
