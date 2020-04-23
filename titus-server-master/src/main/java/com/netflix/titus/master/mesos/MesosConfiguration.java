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

package com.netflix.titus.master.mesos;

import java.util.Set;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.master.mesos.kubeapiserver.KubeConstants;

@Configuration(prefix = "titus.mesos")
public interface MesosConfiguration {

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

    @DefaultValue("false")
    boolean isAllowReconcilerUpdatesForUnknownTasks();

    /**
     * @return whether or not the nested containers should be allowed.
     */
    @DefaultValue("false")
    boolean isNestedContainersEnabled();

    /**
     * @return the min that can be set on the killWaitSeconds field. The default value will be used instead if the value specified
     * is lower than the min.
     */
    @DefaultValue("10")
    int getMinKillWaitSeconds();

    /**
     * @return the max that can be set on the killWaitSeconds field. The default value will be used instead if the value specified
     * is greater than the max.
     */
    @DefaultValue("300")
    int getMaxKillWaitSeconds();

    /**
     * @return maximum amount of seconds to wait before forcefully terminating a container.
     */
    @DefaultValue("10")
    int getDefaultKillWaitSeconds();

    /**
     * @return whether or not to override the executor URI on an agent.
     */
    @DefaultValue("true")
    boolean isExecutorUriOverrideEnabled();

    /**
     * @return the URI to use for all executor URI overrides.
     */
    @DefaultValue("")
    String getGlobalExecutorUriOverride();

    /**
     * @return the command to use with overridden executor URIs.
     */
    @DefaultValue("./run")
    String getExecutorUriOverrideCommand();

    /**
     * @return the registry URL that will be merged with the image name.
     */
    @DefaultValue("")
    String getRegistryUrl();

    /**
     * @return whether or not the kube api server integration is enabled. Only applied at startup.
     */
    @DefaultValue("false")
    boolean isKubeApiServerIntegrationEnabled();

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
     * @return whether or not to GC unknown pods
     */
    @DefaultValue("true")
    boolean isGcUnknownPodsEnabled();

    /**
     * @return whether or not to add a json encoded job descriptor as a pod annotation
     */
    @DefaultValue("true")
    boolean isJobDescriptorAnnotationEnabled();

    /**
     * Taints tolerated by Fenzo. Nodes with taints not in this set are not added to Fenzo. The exception is
     * {@link KubeConstants#TAINT_SCHEDULER} which is checked explicitly.
     */
    Set<String> getFenzoTaintTolerations();
}
