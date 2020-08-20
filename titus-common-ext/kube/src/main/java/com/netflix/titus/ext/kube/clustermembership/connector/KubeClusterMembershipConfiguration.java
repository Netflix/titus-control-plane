/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

import static com.netflix.titus.ext.kube.clustermembership.connector.KubeClusterMembershipConfiguration.PREFIX;

@Configuration(prefix = PREFIX)
public interface KubeClusterMembershipConfiguration {

    String PREFIX = "titus.ext.kube";

    String getKubeApiServerUri();

    /**
     * @return the path to the kubeconfig file
     */
    @DefaultValue("/run/kubernetes/config")
    String getKubeConfigPath();

    @DefaultValue("titus")
    String getNamespace();

    @DefaultValue("titus")
    String getClusterName();

    @DefaultValue("10")
    long getReconcilerQuickCycleMs();

    @DefaultValue("100")
    long getReconcilerLongCycleMs();

    @DefaultValue("30000")
    long getReRegistrationIntervalMs();

    /**
     * Each cluster member periodically updates its registration entry. As the data in Kubernetes do not have
     * TTL associated with them, an entry belonging to a terminated node will stay there until it is removed by
     * the garbage collection process. This property defines a threshold, above which registration entries are
     * regarded as stale and not advertised.
     */
    @DefaultValue("180000")
    long getRegistrationStaleThresholdMs();

    /**
     * Data staleness threshold above which the registration data are removed.
     */
    @DefaultValue("600000")
    long getRegistrationCleanupThresholdMs();

    @DefaultValue("500")
    long getKubeReconnectIntervalMs();

    @DefaultValue("10000")
    long getLeaseDurationMs();
}
