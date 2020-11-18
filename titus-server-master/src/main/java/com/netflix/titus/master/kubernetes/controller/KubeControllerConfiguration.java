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

package com.netflix.titus.master.kubernetes.controller;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.kubernetes.controller")
public interface KubeControllerConfiguration {

    /**
     * @return the accountId to use for GC.
     */
    @DefaultValue("")
    String getAccountId();

    /**
     * @return whether or not the logic to verify that a node's accountId matches the control plane's accountId is enabled.
     */
    @DefaultValue("true")
    boolean isVerifyNodeAccountIdEnabled();

    /**
     * @return the grace period of how old the Ready condition's timestamp can be before attempting to GC a node.
     */
    @DefaultValue("300000")
    long getNodeGcGracePeriodMs();

    /**
     * Amount of time to wait before GC'ing a pod that is past its deletion timestamp. .
     */
    @DefaultValue("1800000")
    long getPodsPastTerminationGracePeriodMs();

    /**
     * Amount of time to wait after the pod creation timestamp for unknown pods before deleting the pod.
     */
    @DefaultValue("300000")
    long getPodUnknownGracePeriodMs();

    /**
     * Amount of time to wait after the pod finished timestamp for terminal pods before deleting the pod.
     */
    @DefaultValue("300000")
    long getPodTerminalGracePeriodMs();

    /**
     * Amount of time to wait for a persistent volume that has not been associated with any active job
     * before deleting the persistent volume.
     */
    @DefaultValue("300000")
    long getPersistentVolumeUnassociatedGracePeriodMs();
}
