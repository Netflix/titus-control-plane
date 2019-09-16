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

import static com.netflix.titus.ext.kube.clustermembership.connector.KubeConnectorConfiguration.PREFIX;

@Configuration(prefix = PREFIX)
public interface KubeConnectorConfiguration {

    String PREFIX = "titus.ext.kube";

    String getK8ApiServerUri();

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

    @DefaultValue("500")
    long getK8ReconnectIntervalMs();

    @DefaultValue("10000")
    long getLeaseDurationMs();
}
