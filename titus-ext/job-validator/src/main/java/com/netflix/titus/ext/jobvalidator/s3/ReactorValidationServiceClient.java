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

package com.netflix.titus.ext.jobvalidator.s3;

import com.netflix.compute.validator.protogen.ComputeValidator.EbsVolumeValidationRequest;
import com.netflix.compute.validator.protogen.ComputeValidator.EbsVolumeValidationResponse;
import com.netflix.compute.validator.protogen.ComputeValidator.IamRoleValidationRequest;
import com.netflix.compute.validator.protogen.ComputeValidator.IamRoleValidationResponse;
import com.netflix.compute.validator.protogen.ComputeValidator.ImageValidationRequest;
import com.netflix.compute.validator.protogen.ComputeValidator.ImageValidationResponse;
import com.netflix.compute.validator.protogen.ComputeValidator.S3BucketAccessValidationRequest;
import com.netflix.compute.validator.protogen.ComputeValidator.S3BucketAccessValidationResponse;
import com.netflix.compute.validator.protogen.ComputeValidator.SecurityGroupValidationRequest;
import com.netflix.compute.validator.protogen.ComputeValidator.SecurityGroupValidationResponse;
import reactor.core.publisher.Mono;

public interface ReactorValidationServiceClient {

    // Validates the given security group. If valid, returns all available representations.
    Mono<SecurityGroupValidationResponse> validateSecurityGroup(SecurityGroupValidationRequest request);

    // Validates IAM role.
    Mono<IamRoleValidationResponse> validateIamRole(IamRoleValidationRequest request);

    // Checks access rights to a bucket with the give IAM role.
    Mono<S3BucketAccessValidationResponse> validateS3BucketAccess(S3BucketAccessValidationRequest request);

    // Validates image.
    Mono<ImageValidationResponse> validateImage(ImageValidationRequest request);

    // Validate EBS volume IDs.
    Mono<EbsVolumeValidationResponse> validateEbsVolume(EbsVolumeValidationRequest request);
}
