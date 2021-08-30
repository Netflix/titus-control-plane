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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.kubernetes.pod.affinity.DefaultPodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.resourcepool.ExplicitJobPodResourcePoolResolver;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobEbsVolumeGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.model.job.JobIpAllocationGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1LabelSelectorRequirement;
import io.kubernetes.client.openapi.models.V1NodeSelector;
import io.kubernetes.client.openapi.models.V1NodeSelectorRequirement;
import io.kubernetes.client.openapi.models.V1PodAffinityTerm;
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class DefaultPodAffinityFactoryTest {

    private static final String DEFAULT_GPU_INSTANCE_TYPE = "p3.2xlarge";
    private static final String SPECIFIC_GPU_INSTANCE_TYPE = "p4.2xlarge";

    private final KubePodConfiguration configuration = Mockito.mock(KubePodConfiguration.class);
    private final FeatureActivationConfiguration featureConfiguration = Mockito.mock(FeatureActivationConfiguration.class);

    private final DefaultPodAffinityFactory factory = new DefaultPodAffinityFactory(configuration, featureConfiguration, new ExplicitJobPodResourcePoolResolver(), TitusRuntimes.test());

    @Before
    public void setUp() throws Exception {
        when(configuration.getDefaultGpuInstanceTypes()).thenReturn(Collections.singletonList(DEFAULT_GPU_INSTANCE_TYPE));
        when(featureConfiguration.isRelocationBinpackingEnabled()).thenReturn(true);
    }

    @Test
    public void testInstanceTypeAffinity() {
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(
                newJobWithHardConstraint(JobConstraints.MACHINE_TYPE.toUpperCase(), "r5.metal"), JobGenerator.oneBatchTask()
        );
        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
    }

    @Test
    public void testKubeBackendAffinity() {
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(
                newJobWithHardConstraint(JobConstraints.KUBE_BACKEND, "kublet"), JobGenerator.oneBatchTask()
        );
        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        V1NodeSelectorRequirement requirement = nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(requirement.getKey()).isEqualTo(KubeConstants.TAINT_KUBE_BACKEND);
        assertThat(requirement.getValues()).contains("kublet");
    }

    @Test
    public void testEmptyInstanceTypeIsIgnored() {
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(
                newJobWithHardConstraint(JobConstraints.MACHINE_TYPE, ""), JobGenerator.oneBatchTask()
        );
        assertThat(affinityWithAnnotations.getLeft().getNodeAffinity()).isNull();
    }

    @Test
    public void testDefaultGpuInstanceAssignment() {
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(
                newGpuJob(Collections.emptyMap()), JobGenerator.oneBatchTask()
        );

        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(DEFAULT_GPU_INSTANCE_TYPE);
    }

    @Test
    public void testSpecificGpuInstanceAssignment() {
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(newGpuJob(Collections.singletonMap(
                JobConstraints.MACHINE_TYPE, SPECIFIC_GPU_INSTANCE_TYPE
        )), JobGenerator.oneBatchTask());

        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(SPECIFIC_GPU_INSTANCE_TYPE);
    }

    @Test
    public void testResourcePoolAffinity() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder().withJobDescriptor(JobFunctions.appendJobDescriptorAttributes(job.getJobDescriptor(),
                Collections.singletonMap(JobAttributes.JOB_PARAMETER_RESOURCE_POOLS, "elastic"))
        ).build();
        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(job, JobGenerator.oneBatchTask());

        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getKey()).isEqualTo(KubeConstants.NODE_LABEL_RESOURCE_POOL);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo("elastic");
    }

    @Test
    public void testEbsVolumeAzAffinity() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        List<EbsVolume> ebsVolumes = JobEbsVolumeGenerator.jobEbsVolumes(1).toList();
        Map<String, String> ebsVolumeAttributes = JobEbsVolumeGenerator.jobEbsVolumesToAttributes(ebsVolumes);
        job = job.toBuilder().withJobDescriptor(JobFunctions.jobWithEbsVolumes(job.getJobDescriptor(), ebsVolumes, ebsVolumeAttributes)).build();

        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(job, JobEbsVolumeGenerator.appendEbsVolumeAttribute(JobGenerator.oneBatchTask(), ebsVolumes.get(0).getVolumeId()));
        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getKey()).isEqualTo(KubeConstants.NODE_LABEL_ZONE);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(ebsVolumes.get(0).getVolumeAvailabilityZone());
    }

    @Test
    public void testIpAllocationAzAffinity() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        List<SignedIpAddressAllocation> ipAddressAllocations = JobIpAllocationGenerator.jobIpAllocations(1).toList();
        job = job.toBuilder().withJobDescriptor(JobFunctions.jobWithIpAllocations(job.getJobDescriptor(), ipAddressAllocations)).build();

        Pair<V1Affinity, Map<String, String>> affinityWithAnnotations = factory.buildV1Affinity(job, JobIpAllocationGenerator.appendIpAllocationAttribute(JobGenerator.oneBatchTask(), ipAddressAllocations.get(0).getIpAddressAllocation().getAllocationId()));
        V1NodeSelector nodeSelector = affinityWithAnnotations.getLeft().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getKey()).isEqualTo(KubeConstants.NODE_LABEL_ZONE);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(ipAddressAllocations.get(0).getIpAddressAllocation().getIpAddressLocation().getAvailabilityZone());
    }

    @Test
    public void relocationBinPacking() {
        Job<ServiceJobExt> job = JobGenerator.oneServiceJob();
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().but(
                jd -> jd.getDisruptionBudget().toBuilder()
                        .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder().build())
        )).build();
        ServiceJobTask task = JobGenerator.oneServiceTask();
        V1Affinity affinity = factory.buildV1Affinity(job, task).getLeft();
        List<V1WeightedPodAffinityTerm> podAffinityTerms = affinity.getPodAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();
        assertThat(podAffinityTerms).hasSize(1);
        V1PodAffinityTerm podAffinityTerm = podAffinityTerms.get(0).getPodAffinityTerm();
        assertThat(podAffinityTerm.getTopologyKey()).isEqualTo(KubeConstants.NODE_LABEL_MACHINE_ID);
        assertThat(podAffinityTerm.getLabelSelector().getMatchExpressions()).hasSize(1);
        V1LabelSelectorRequirement affinityRequirement = podAffinityTerm.getLabelSelector().getMatchExpressions().get(0);
        assertThat(affinityRequirement.getKey()).isEqualTo(KubeConstants.POD_LABEL_RELOCATION_BINPACK);
        assertThat(affinityRequirement.getOperator()).isEqualTo(KubeConstants.SELECTOR_OPERATOR_EXISTS);

        List<V1WeightedPodAffinityTerm> antiAffinityTerms = affinity.getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();
        assertThat(antiAffinityTerms).hasSize(1);
        V1PodAffinityTerm antiAffinityTerm = antiAffinityTerms.get(0).getPodAffinityTerm();
        assertThat(antiAffinityTerm.getTopologyKey()).isEqualTo(KubeConstants.NODE_LABEL_MACHINE_ID);
        assertThat(antiAffinityTerm.getLabelSelector().getMatchExpressions()).hasSize(1);
        V1LabelSelectorRequirement antiAffinityRequirement = antiAffinityTerm.getLabelSelector().getMatchExpressions().get(0);
        assertThat(antiAffinityRequirement.getKey()).isEqualTo(KubeConstants.POD_LABEL_RELOCATION_BINPACK);
        assertThat(antiAffinityRequirement.getOperator()).isEqualTo(KubeConstants.SELECTOR_OPERATOR_DOES_NOT_EXIST);
    }

    @Test
    public void relocationBinPackingNegative() {
        Job<ServiceJobExt> job = JobGenerator.oneServiceJob();
        ServiceJobTask task = JobGenerator.oneServiceTask();
        V1Affinity affinity = factory.buildV1Affinity(job, task).getLeft();
        assertThat(affinity.getPodAffinity()).isNull();
        assertThat(affinity.getPodAntiAffinity()).isNull();
    }

    private Job<BatchJobExt> newJobWithHardConstraint(String name, String value) {
        return JobFunctions.appendHardConstraint(JobGenerator.oneBatchJob(), name, value);
    }

    private Job<BatchJobExt> newGpuJob(Map<String, String> hardConstraints) {
        Job<BatchJobExt> template = JobGenerator.oneBatchJob();
        JobDescriptor<BatchJobExt> jobDescriptor = template.getJobDescriptor();
        Container container = jobDescriptor.getContainer();

        return template.toBuilder()
                .withJobDescriptor(jobDescriptor.toBuilder()
                        .withContainer(container.toBuilder()
                                .withContainerResources(container.getContainerResources().toBuilder().withGpu(1).build())
                                .withHardConstraints(hardConstraints)
                                .build()
                        )
                        .build()
                )
                .build();
    }
}