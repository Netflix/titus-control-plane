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

    String getRegion();

    /**
     * Titus master ASG name.
     */
    @DefaultValue("TitusMasterAsgName")
    String getTitusMasterAsgName();

    /**
     * Fetching all instance groups in {@link AwsInstanceCloudConnector#getInstanceGroups()} method may require
     * multiple page queries (AWS calls), so we need to set a high timeout value.
     */
    @DefaultValue("300000")
    long getInstanceGroupsFetchTimeoutMs();

    /**
     * Fetching all instances by instance group id in {@link AwsInstanceCloudConnector#getInstancesByInstanceGroupId(String)} ()}
     * method may require multiple page queries (AWS calls), so we need to set a timeout value.
     */
    @DefaultValue("60000")
    long getInstancesByInstanceGroupIdFetchTimeoutMs();

    /**
     * Except {@link AwsInstanceCloudConnector#getInstanceGroups()}, all the remaining operations require single
     * AWS request, hence timeout value ~10sec should be more than enough.
     */
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
}
