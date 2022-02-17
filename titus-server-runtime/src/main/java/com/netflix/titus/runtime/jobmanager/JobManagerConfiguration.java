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

package com.netflix.titus.runtime.jobmanager;

import java.util.List;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.jobManager")
public interface JobManagerConfiguration {

    @DefaultValue("_none_")
    String getDefaultIamRole();

    List<String> getDefaultSecurityGroups();

    /**
     * @return A comma separated string of one or more subnets to launch the container in. This string is set as an annotation on the pod.
     */
    String getDefaultSubnets();

    /**
     * @return Default account to launch containers in. This value is used when not explicitly provided by the caller.
     */
    String getDefaultContainerAccountId();

    @DefaultValue("_none_")
    String getNoncompliantClientWhiteList();

    /**
     * @return the minimum disk size in megabytes that the disk resource dimension should be set to.
     */
    @DefaultValue("10000")
    int getMinDiskSizeMB();
}
