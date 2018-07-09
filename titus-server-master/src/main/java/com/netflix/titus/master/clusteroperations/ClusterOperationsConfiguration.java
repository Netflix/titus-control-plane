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

package com.netflix.titus.master.clusteroperations;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.clusterOperations")
public interface ClusterOperationsConfiguration {

    /**
     * @return whether or not the cluster agent remover should remove agents.
     */
    @DefaultValue("true")
    boolean isRemovingAgentsEnabled();

    /**
     * @return the grace period in milliseconds before the remover will start removing instances in an instance group.
     */
    @DefaultValue("300000")
    long getInstanceGroupRemovableGracePeriodMs();
}