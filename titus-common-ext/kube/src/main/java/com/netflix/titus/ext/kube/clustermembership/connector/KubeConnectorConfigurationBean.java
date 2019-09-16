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

import com.netflix.titus.common.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

public class KubeConnectorConfigurationBean implements KubeConnectorConfiguration {

    private final Environment environment;
    private final String prefix;

    public KubeConnectorConfigurationBean(Environment environment, String prefix) {
        this.environment = environment;
        this.prefix = prefix.endsWith(".") ? prefix : prefix + '.';
    }

    @Override
    public String getK8ApiServerUri() {
        return SpringConfigurationUtil.getString(environment, prefix + "k8ApiServerUri", "");
    }

    @Override
    public String getNamespace() {
        return SpringConfigurationUtil.getString(environment, prefix + "namespace", "titus");
    }

    @Override
    public String getClusterName() {
        return SpringConfigurationUtil.getString(environment, prefix + "clusterName", "titus");
    }

    @Override
    public long getReconcilerQuickCycleMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "reconcilerQuickCycleMs", 10);
    }

    @Override
    public long getReconcilerLongCycleMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "reconcilerLongCycleMs", 100);
    }

    @Override
    public long getReRegistrationIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "reRegistrationIntervalMs", 30_000);
    }

    @Override
    public long getK8ReconnectIntervalMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "k8ReconnectIntervalMs", 500);
    }

    @Override
    public long getLeaseDurationMs() {
        return SpringConfigurationUtil.getLong(environment, prefix + "leaseDurationMs", 10_000);
    }
}
