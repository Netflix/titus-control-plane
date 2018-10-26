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
package com.netflix.titus.ext.aws.appscale;

import com.amazonaws.services.applicationautoscaling.model.ScalableTarget;
import com.netflix.titus.api.appscale.model.AutoScalableTarget;

public class AWSAppAutoScalingUtil {
    public static String buildGatewayResourceId(String jobId, String awsGatewayEndpointPrefix, String region, String stack) {
        return String.format("https://%s.execute-api.%s.amazonaws.com/%s/scalableTargetDimensions/%s",
                awsGatewayEndpointPrefix, region, stack, jobId);
    }

    public static String buildScalingPolicyName(String policyRefId, String jobId) {
        return String.format("%s/%s", jobId, policyRefId);
    }

    public static AutoScalableTarget toAutoScalableTarget(ScalableTarget scalableTarget) {
        return AutoScalableTarget.newBuilder()
                .withResourceId(scalableTarget.getResourceId())
                .withMinCapacity(scalableTarget.getMinCapacity())
                .withMaxCapacity(scalableTarget.getMaxCapacity())
                .build();
    }
}
