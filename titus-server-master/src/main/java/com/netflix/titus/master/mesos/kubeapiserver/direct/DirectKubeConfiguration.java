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

import java.util.List;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.CapacityGroupPodResourcePoolResolver;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;

@Configuration(prefix = "titusMaster.directKube")
public interface DirectKubeConfiguration extends KubeConnectorConfiguration {

    /**
     * @return the registry URL that will be merged with the image name.
     */
    @DefaultValue("")
    String getRegistryUrl();

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
     * Thread pool size for handling Kube apiClient calls.
     */
    @DefaultValue("20")
    int getApiClientThreadPoolSize();

    @DefaultValue("5000")
    long getKubeApiClientTimeoutMs();

    @DefaultValue("false")
    boolean isAsyncApiEnabled();

    /**
     * Get list of farzone names. A job associated with a zone hard constraint, where the zone id is one of the
     * farzone names has pods tainted with that zone id.
     */
    List<String> getFarzones();

    /**
     * A list of primary/default availability zones.
     */
    List<String> getPrimaryZones();

    /**
     * @return whether or not to add a json encoded job descriptor as a pod annotation
     */
    @DefaultValue("true")
    boolean isJobDescriptorAnnotationEnabled();

    /**
     * Default GPU instance types assigned to a pod requesting GPU resources, but not specifying directly which GPU
     * instance to use. If empty, no GPU instance type constraint is set.
     */
    List<String> getDefaultGpuInstanceTypes();

    @DefaultValue("default-scheduler")
    String getKubeSchedulerName();

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
     * Amount of time to wait before a pod for a completed task should be removed.
     */
    @DefaultValue("0")
    long getTerminatedPodGcDelayMs();

    /**
     * The grace period on the pod object to use when the pod is created.
     */
    @DefaultValue("600")
    long getPodTerminationGracePeriodSeconds();

    /**
     * Amount of grace period to set when deleting a namespace pod.
     */
    @DefaultValue("300")
    int getDeleteGracePeriodSeconds();

    /**
     * Amount of time past the last node ready condition heartbeat to wait until a node is deleted.
     */
    @DefaultValue("300000")
    long getNodeGcTtlMs();

    /**
     * Maximum number of concurrent pod create requests.
     */
    @DefaultValue("200")
    int getPodCreateConcurrencyLimit();

    /**
     * Frequency at which {@link CapacityGroupPodResourcePoolResolver} resolve configuration is re-read.
     */
    @DefaultValue("5000")
    long getCapacityGroupPodResourcePoolResolverUpdateIntervalMs();

    /**
     * Set to true to enable resource pool affinity placement constraints.
     */
    @DefaultValue("false")
    boolean isResourcePoolAffinityEnabled();

    @DefaultValue("")
    String getGpuResourcePoolNames();

    /**
     * Set to true to enable S3 writer configuration.
     */
    @DefaultValue("false")
    boolean isDefaultS3WriterRoleEnabled();

    /**
     * Default IAM role that should be used to upload log files to S3 bucket.
     */
    String getDefaultS3WriterRole();

    /**
     * Set to true to enable setting pod resources in byte units as defined in
     * https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/.
     */
    @DefaultValue("false")
    boolean isBytePodResourceEnabled();
}
