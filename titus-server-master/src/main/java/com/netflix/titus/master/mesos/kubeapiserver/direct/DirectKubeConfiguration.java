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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;

@Configuration(prefix = "titusMaster.directKube")
public interface DirectKubeConfiguration extends KubeConnectorConfiguration {

    /**
     * Thread pool size for handling Kube apiClient calls.
     */
    @DefaultValue("20")
    int getApiClientThreadPoolSize();

    @DefaultValue("5000")
    long getKubeApiClientTimeoutMs();

    @DefaultValue("true")
    boolean isAsyncApiEnabled();

    /**
     * Regular expression to match pod create errors for rejected pods.
     */
    @DefaultValue(".*")
    String getInvalidPodMessagePattern();

    /**
     * Regular expression to match pod create errors there are recoverable.
     */
    @DefaultValue("NONE")
    String getTransientSystemErrorMessagePattern();

    /**
     * Amount of grace period to set when deleting a namespace pod.
     */
    @DefaultValue("300")
    int getDeleteGracePeriodSeconds();

    /**
     * Maximum number of concurrent pod create requests.
     */
    @DefaultValue("200")
    int getPodCreateConcurrencyLimit();

    /**
     * Set to true to enable EBS PV and PVC management.
     */
    @DefaultValue("false")
    boolean isEbsVolumePvEnabled();
}
