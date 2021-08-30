/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.affinity;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocationUtils;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.resourcepool.PodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.ResourcePoolAssignment;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1LabelSelectorRequirement;
import io.kubernetes.client.openapi.models.V1NodeAffinity;
import io.kubernetes.client.openapi.models.V1NodeSelector;
import io.kubernetes.client.openapi.models.V1NodeSelectorRequirement;
import io.kubernetes.client.openapi.models.V1NodeSelectorTerm;
import io.kubernetes.client.openapi.models.V1PodAffinity;
import io.kubernetes.client.openapi.models.V1PodAffinityTerm;
import io.kubernetes.client.openapi.models.V1PodAntiAffinity;
import io.kubernetes.client.openapi.models.V1PreferredSchedulingTerm;
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm;

import static com.netflix.titus.common.util.CollectionsExt.toLowerCaseKeys;

@Singleton
public class DefaultPodAffinityFactory implements PodAffinityFactory {

    private static final int EXCLUSIVE_HOST_WEIGHT = 100;
    private static final int UNIQUE_HOST_WEIGHT = 100;
    private static final int NODE_AFFINITY_WEIGHT = 100;
    private static final int RELOCATION_AFFINITY_WEIGHT = 50;

    private final KubePodConfiguration configuration;
    private final FeatureActivationConfiguration featureConfiguration;
    private final PodResourcePoolResolver podResourcePoolResolver;
    private final TitusRuntime titusRuntime;

    @Inject
    public DefaultPodAffinityFactory(KubePodConfiguration configuration,
                                     FeatureActivationConfiguration featureConfiguration,
                                     PodResourcePoolResolver podResourcePoolResolver,
                                     TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.featureConfiguration = featureConfiguration;
        this.podResourcePoolResolver = podResourcePoolResolver;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Pair<V1Affinity, Map<String, String>> buildV1Affinity(Job<?> job, Task task) {
        return new Processor(job, task).build();
    }

    private class Processor {

        private final Job<?> job;
        private final Task task;
        private final V1Affinity v1Affinity;
        private final Map<String, String> annotations = new HashMap<>();

        private Processor(Job<?> job, Task task) {
            this.job = job;
            this.task = task;
            this.v1Affinity = new V1Affinity();

            processJobConstraints(toLowerCaseKeys(job.getJobDescriptor().getContainer().getHardConstraints()), true);
            processJobConstraints(toLowerCaseKeys(job.getJobDescriptor().getContainer().getSoftConstraints()), false);
            processResourcePoolConstraints();
            processZoneConstraints();
            processRelocationAffinity();
        }

        private void processJobConstraints(Map<String, String> constraints, boolean hard) {
            if (Boolean.parseBoolean(constraints.get(JobConstraints.EXCLUSIVE_HOST))) {
                addExclusiveHostConstraint(hard);
            }

            if (Boolean.parseBoolean(constraints.get(JobConstraints.UNIQUE_HOST))) {
                addUniqueHostConstraint(hard);
            }

            if (constraints.containsKey(JobConstraints.AVAILABILITY_ZONE)) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_ZONE, constraints.get(JobConstraints.AVAILABILITY_ZONE), hard);
            }

            if (constraints.containsKey(JobConstraints.MACHINE_GROUP)) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_MACHINE_GROUP, constraints.get(JobConstraints.MACHINE_GROUP), hard);
            }

            if (constraints.containsKey(JobConstraints.MACHINE_ID)) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_MACHINE_ID, constraints.get(JobConstraints.MACHINE_ID), hard);
            }

            if (constraints.containsKey(JobConstraints.KUBE_BACKEND)) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_KUBE_BACKEND, constraints.get(JobConstraints.KUBE_BACKEND), hard);
            }

            if (constraints.containsKey(JobConstraints.CPU_MODEL)) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_CPU_MODEL, constraints.get(JobConstraints.CPU_MODEL), hard);
            }

            String instanceType = constraints.get(JobConstraints.MACHINE_TYPE);
            boolean instanceTypeRequested = !StringExt.isEmpty(instanceType);
            if (instanceTypeRequested) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_INSTANCE_TYPE, instanceType, hard);
            }

            if (hard && !instanceTypeRequested) {
                boolean gpuRequested = job.getJobDescriptor().getContainer().getContainerResources().getGpu() > 0;
                List<String> defaultGpuInstanceTypes = configuration.getDefaultGpuInstanceTypes();
                if (gpuRequested && !defaultGpuInstanceTypes.isEmpty()) {
                    // If not explicit instance type requested, restrict GPU instance types to a default set.
                    addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_INSTANCE_TYPE, defaultGpuInstanceTypes, true);
                }
            }
        }

        private void addExclusiveHostConstraint(boolean hard) {
            V1PodAffinityTerm term = new V1PodAffinityTerm()
                    .labelSelector(new V1LabelSelector()
                            .addMatchExpressionsItem(new V1LabelSelectorRequirement()
                                    .key(KubeConstants.POD_LABEL_TASK_ID)
                                    .operator(KubeConstants.SELECTOR_OPERATOR_EXISTS)
                            ))
                    .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID);

            if (hard) {
                getPodAntiAffinity().addRequiredDuringSchedulingIgnoredDuringExecutionItem(term);
            } else {
                getPodAntiAffinity().addPreferredDuringSchedulingIgnoredDuringExecutionItem(
                        new V1WeightedPodAffinityTerm()
                                .weight(EXCLUSIVE_HOST_WEIGHT)
                                .podAffinityTerm(term)
                );
            }
        }

        private void addUniqueHostConstraint(boolean hard) {
            V1PodAffinityTerm term = new V1PodAffinityTerm()
                    .labelSelector(new V1LabelSelector()
                            .addMatchExpressionsItem(new V1LabelSelectorRequirement()
                                    .key(KubeConstants.POD_LABEL_JOB_ID)
                                    .operator(KubeConstants.SELECTOR_OPERATOR_IN)
                                    .values(Collections.singletonList(job.getId()))
                            ))
                    .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID);

            if (hard) {
                getPodAntiAffinity().addRequiredDuringSchedulingIgnoredDuringExecutionItem(term);
            } else {
                getPodAntiAffinity().addPreferredDuringSchedulingIgnoredDuringExecutionItem(
                        new V1WeightedPodAffinityTerm()
                                .weight(UNIQUE_HOST_WEIGHT)
                                .podAffinityTerm(term)
                );
            }
        }

        private void addNodeAffinitySelectorConstraint(String key, String value, boolean hard) {
            addNodeAffinitySelectorConstraint(key, Collections.singletonList(value), hard);
        }

        private void addNodeAffinitySelectorConstraint(String key, List<String> values, boolean hard) {
            if (hard) {
                addNodeAffinityRequiredSelectorConstraint(key, values);
            } else {
                addNodeAffinityPreferredSelectorConstraint(key, values);
            }
        }

        private void addNodeAffinityRequiredSelectorConstraint(String key, List<String> values) {
            V1NodeSelectorRequirement requirement = new V1NodeSelectorRequirement()
                    .key(key)
                    .operator(KubeConstants.SELECTOR_OPERATOR_IN)
                    .values(values);

            V1NodeSelector nodeSelector = getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            if (nodeSelector == null) {
                getNodeAffinity().requiredDuringSchedulingIgnoredDuringExecution(nodeSelector = new V1NodeSelector());
            }
            if (nodeSelector.getNodeSelectorTerms().isEmpty()) {
                nodeSelector.addNodeSelectorTermsItem(new V1NodeSelectorTerm().addMatchExpressionsItem(requirement));
            } else {
                nodeSelector.getNodeSelectorTerms().get(0).addMatchExpressionsItem(requirement);
            }
        }

        private void addNodeAffinityPreferredSelectorConstraint(String key, List<String> values) {
            List<V1PreferredSchedulingTerm> nodeSelector = getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();

            V1NodeSelectorTerm term;
            if (nodeSelector == null) {
                V1PreferredSchedulingTerm preferredTerm = new V1PreferredSchedulingTerm()
                        .preference(term = new V1NodeSelectorTerm())
                        .weight(NODE_AFFINITY_WEIGHT);
                getNodeAffinity().addPreferredDuringSchedulingIgnoredDuringExecutionItem(preferredTerm);
            } else {
                term = getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPreference();
            }

            V1NodeSelectorRequirement requirement = new V1NodeSelectorRequirement()
                    .key(key)
                    .operator(KubeConstants.SELECTOR_OPERATOR_IN)
                    .values(values);

            term.addMatchExpressionsItem(requirement);
        }

        private void processResourcePoolConstraints() {
            List<ResourcePoolAssignment> resourcePools = podResourcePoolResolver.resolve(job, task);
            if (resourcePools.isEmpty()) {
                return;
            }

            List<String> names = resourcePools.stream().map(ResourcePoolAssignment::getResourcePoolName).collect(Collectors.toList());
            String rule = resourcePools.isEmpty() ? "none" :
                    (resourcePools.size() == 1
                            ? resourcePools.get(0).getRule()
                            : resourcePools.stream().map(ResourcePoolAssignment::getRule).collect(Collectors.joining(","))
                    );

            addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_RESOURCE_POOL, names, true);
            annotations.put(KubeConstants.TITUS_SCALER_DOMAIN + "resource-pool-selection", rule);
            annotations.put(KubeConstants.NODE_LABEL_RESOURCE_POOL, String.join(",", names));
        }

        private void processZoneConstraints() {
            // If we have a single zone hard constraint defined, there is no need to add anything on top of this.
            String zone = JobFunctions.findHardConstraint(job, JobConstraints.AVAILABILITY_ZONE).orElse("");
            if (!StringExt.isEmpty(zone)) {
                return;
            }

            // If there is a Static IP allocation, get its zone
            Optional<String> optionalIpAllocationZone = IpAddressAllocationUtils.getIpAllocationZoneForTask(job.getJobDescriptor(), task, titusRuntime.getCodeInvariants());

            // If there is an EBS volume, get its zone
            Optional<String> optionalEbsVolumeZone = EbsVolumeUtils.getEbsVolumeForTask(job, task)
                    .map(EbsVolume::getVolumeAvailabilityZone);

            if (optionalIpAllocationZone.isPresent() && optionalEbsVolumeZone.isPresent()) {
                String ipAllocationZone = optionalIpAllocationZone.get();
                String ebsVolumeZone = optionalEbsVolumeZone.get();
                // If the zones for the two assigned resources do not match we do not assign either.
                if (ebsVolumeZone.equals(ipAllocationZone)) {
                    addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_ZONE, ebsVolumeZone, true);
                } else {
                    titusRuntime.getCodeInvariants().inconsistent("Task %s has assigned Static IP in zone %s but assigned EBS Volume in zone %s", task.getId(), ipAllocationZone, ebsVolumeZone);
                }
            } else if (optionalIpAllocationZone.isPresent()) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_ZONE, optionalIpAllocationZone.get(), true);
            } else {
                optionalEbsVolumeZone.ifPresent(ebsVolumeZone -> addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_ZONE, ebsVolumeZone, true));
            }

            // If there is no zone hard constraint, it defaults to placement in the primary availability zones
            if (!configuration.getPrimaryZones().isEmpty()) {
                addNodeAffinitySelectorConstraint(KubeConstants.NODE_LABEL_ZONE, configuration.getPrimaryZones(), true);
            }
        }

        private void processRelocationAffinity() {
            if (!featureConfiguration.isRelocationBinpackingEnabled() || !shouldBinpackForRelocation()) {
                return;
            }
            getPodAffinity().addPreferredDuringSchedulingIgnoredDuringExecutionItem(
                    new V1WeightedPodAffinityTerm()
                            .weight(RELOCATION_AFFINITY_WEIGHT)
                            .podAffinityTerm(new V1PodAffinityTerm()
                                    .labelSelector(new V1LabelSelector()
                                            .addMatchExpressionsItem(new V1LabelSelectorRequirement()
                                                    .key(KubeConstants.POD_LABEL_RELOCATION_BINPACK)
                                                    .operator(KubeConstants.SELECTOR_OPERATOR_EXISTS)
                                            )
                                    )
                                    .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID)
                            )
            );
            getPodAntiAffinity().addPreferredDuringSchedulingIgnoredDuringExecutionItem(
                    new V1WeightedPodAffinityTerm()
                            .weight(RELOCATION_AFFINITY_WEIGHT)
                            .podAffinityTerm(new V1PodAffinityTerm()
                                    .labelSelector(new V1LabelSelector()
                                            .addMatchExpressionsItem(new V1LabelSelectorRequirement()
                                                    .key(KubeConstants.POD_LABEL_RELOCATION_BINPACK)
                                                    .operator(KubeConstants.SELECTOR_OPERATOR_DOES_NOT_EXIST)
                                            )
                                    )
                                    .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID)
                            )
            );
        }

        private boolean shouldBinpackForRelocation() {
            return job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy;
        }

        private V1NodeAffinity getNodeAffinity() {
            if (v1Affinity.getNodeAffinity() == null) {
                v1Affinity.nodeAffinity(new V1NodeAffinity());
            }
            return v1Affinity.getNodeAffinity();
        }

        private V1PodAffinity getPodAffinity() {
            if (v1Affinity.getPodAffinity() == null) {
                v1Affinity.podAffinity(new V1PodAffinity());
            }
            return v1Affinity.getPodAffinity();
        }

        private V1PodAntiAffinity getPodAntiAffinity() {
            if (v1Affinity.getPodAntiAffinity() == null) {
                v1Affinity.podAntiAffinity(new V1PodAntiAffinity());
            }
            return v1Affinity.getPodAntiAffinity();
        }

        private Pair<V1Affinity, Map<String, String>> build() {
            return Pair.of(v1Affinity, annotations);
        }
    }
}
