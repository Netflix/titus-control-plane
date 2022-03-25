/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.kubernetes")
public interface KubernetesConfiguration {

    @DefaultValue(".*invalidRequest.*")
    String getInvalidRequestMessagePattern();

    @DefaultValue(".*crashed.*")
    String getCrashedMessagePattern();

    @DefaultValue(".*transientSystemError.*")
    String getTransientSystemErrorMessagePattern();

    @DefaultValue(".*localSystemError.*")
    String getLocalSystemErrorMessagePattern();

    @DefaultValue(".*unknownSystemError.*")
    String getUnknownSystemErrorMessagePattern();

    @DefaultValue("true")
    boolean isReconcilerEnabled();

    @DefaultValue("10000")
    long getReconcilerInitialDelayMs();

    @DefaultValue("30000")
    long getReconcilerIntervalMs();

    @DefaultValue("60000")
    long getOrphanedPodTimeoutMs();

    /**
     * @return the kube api server url to use. If this is empty, use the kube config path instead.
     */
    @DefaultValue("")
    String getKubeApiServerUrl();

    /**
     * @return the path to the kubeconfig file
     */
    @DefaultValue("/run/kubernetes/config")
    String getKubeConfigPath();

    /**
     * @return whether to enable or disable compression when LISTing pods using kubernetes java client
     */
    @DefaultValue("false")
    boolean isCompressionEnabledForKubeApiClient();
}
