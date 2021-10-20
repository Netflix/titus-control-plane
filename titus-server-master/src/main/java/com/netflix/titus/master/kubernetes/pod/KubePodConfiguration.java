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

package com.netflix.titus.master.kubernetes.pod;

import java.util.List;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.master.kubernetes.pod.resourcepool.CapacityGroupPodResourcePoolResolver;

@Configuration(prefix = "titusMaster.kubernetes.pod")
public interface KubePodConfiguration {

    /**
     * @return the registry URL that will be merged with the image name.
     */
    @DefaultValue("")
    String getRegistryUrl();

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
     * @return maximum amount of seconds to wait before forcefully terminating a batch job container.
     */
    @DefaultValue("10")
    int getBatchDefaultKillWaitSeconds();

    /**
     * @return maximum amount of seconds to wait before forcefully terminating a service job container.
     */
    @DefaultValue("120")
    int getServiceDefaultKillWaitSeconds();

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

    @DefaultValue("titus-kube-scheduler-reserved")
    String getReservedCapacityKubeSchedulerName();

    /**
     * This string corresponds to a kube-scheduler profile name which implements exact same set of scheduler plugins as
     * the standard reserved capacity scheduler (titus-kube-scheduler-reserved) except
     * with bin-packing plugins enabled.
     */
    @DefaultValue("titus-kube-scheduler-reserved-binpacking")
    String getReservedCapacityKubeSchedulerNameForBinPacking();

    /**
     * The grace period on the pod object to use when the pod is created.
     */
    @DefaultValue("600")
    long getPodTerminationGracePeriodSeconds();

    /**
     * Set to true to enable resource pool affinity placement constraints.
     */
    @DefaultValue("true")
    boolean isResourcePoolAffinityEnabled();

    /**
     * Comma separated list of GPU resource pool names. Set to empty string if no GPU resource pools are available.
     */
    @DefaultValue("")
    String getGpuResourcePoolNames();

    /**
     * Frequency at which {@link CapacityGroupPodResourcePoolResolver} resolve configuration is re-read.
     */
    @DefaultValue("5000")
    long getCapacityGroupPodResourcePoolResolverUpdateIntervalMs();

    /**
     * Set to true to enable S3 writer configuration.
     */
    @DefaultValue("true")
    boolean isDefaultS3WriterRoleEnabled();

    /**
     * Default IAM role that should be used to upload log files to S3 bucket.
     */
    String getDefaultS3WriterRole();

    /**
     * Set to true to enable setting pod resources in byte units as defined in
     * https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/.
     */
    @DefaultValue("true")
    boolean isBytePodResourceEnabled();

    /**
     * A regular expression pattern for capacity groups for which job spreading should be disabled.
     */
    @DefaultValue("NONE")
    String getDisabledJobSpreadingPattern();

    /**
     * @return the pod spec target region to use
     */
    String getTargetRegion();

    /**
     * @return the pod spec routing rules to use.
     */
    @DefaultValue("v0")
    String getDefaultPodSpecVersion();

    /**
     * @return the pod spec routing rules to use.
     */
    @DefaultValue("")
    String getPodSpecVersionRoutingRules();
}
