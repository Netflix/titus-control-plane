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

import com.netflix.archaius.api.annotations.PropertyName;

public interface AWSAppScalingConfig {
    String SERVICE_NAMESPACE = "custom-resource";
    // AWS requires this field be set to this specific value for all application-autoscaling calls.
    String SCALABLE_DIMENSION = "custom-resource:ResourceType:Property";

    @PropertyName(name = "region")
    String getRegion();

    @PropertyName(name = "netflix.stack")
    String getStack();

    @PropertyName(name = "aws.gateway.api.prefix")
    String getAWSGatewayEndpointPrefix();
}

