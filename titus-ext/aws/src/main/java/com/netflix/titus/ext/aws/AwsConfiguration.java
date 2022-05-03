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

package com.netflix.titus.ext.aws;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.ext.aws")
public interface AwsConfiguration {

    /**
     * If set, this region value is used when building {@link com.amazonaws.services.ec2.AmazonEC2Client} and
     * {@link com.amazonaws.services.autoscaling.AmazonAutoScalingClient}.
     */
    String getDataPlaneRegion();

    String getRegion();

    /**
     * Titus master ASG name.
     */
    @DefaultValue("TitusMasterAsgName")
    String getTitusMasterAsgName();

    @DefaultValue("10000")
    long getAwsRequestTimeoutMs();

    /**
     * Interval at which a cleaner process will run to terminate agents with {@link AwsInstanceCloudConnector#TAG_TERMINATE}
     * tag on them.
     */
    @DefaultValue("600000")
    long getReaperIntervalMs();

    /**
     * Amount of time to cache IAM role records, before making another call to AWS.
     */
    @DefaultValue("60000")
    long getIamRoleCacheTimeoutMs();

    /**
     * IAM role associated with agent instances. Agent instances use this role to assume into container provided
     * IAM roles.
     */
    @DefaultValue("")
    String getDataPlaneAgentRoleArn();

    @DefaultValue("titusControlPlaneSession")
    String getDataPlaneAgentRoleSessionName();

    @DefaultValue("3600")
    int getDataPlaneAgentRoleSessionDurationSeconds();

    /**
     * IAM role ARN to assume into to access AWS API for the data plane account. If not set, it is assumed that
     * the control plane and the data plane run in the same account and no cross access is required.
     */
    @DefaultValue("")
    String getDataPlaneControllerRoleArn();

    @DefaultValue("titusControlPlaneSession")
    String getDataPlaneControllerRoleSessionName();

    @DefaultValue("3600")
    int getDataPlaneControllerRoleSessionDurationSeconds();

    /**
     * IAM role name the control plane uses to manage aws resources such as load balancers.
     */
    @DefaultValue("TitusControlPlaneRole")
    String getControlPlaneRoleName();

    @DefaultValue("titusControlPlaneSession")
    String getControlPlaneRoleSessionName();

    @DefaultValue("3600")
    int getControlPlaneRoleSessionDurationSeconds();
}
