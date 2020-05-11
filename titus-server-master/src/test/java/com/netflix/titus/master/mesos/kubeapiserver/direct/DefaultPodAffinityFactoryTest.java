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
import java.util.Map;

import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.mesos.kubeapiserver.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1NodeSelector;
import io.kubernetes.client.openapi.models.V1NodeSelectorRequirement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class DefaultPodAffinityFactoryTest {

    private static final String DEFAULT_GPU_INSTANCE_TYPE = "p3.2xlarge";
    private static final String SPECIFIC_GPU_INSTANCE_TYPE = "p4.2xlarge";

    private final DirectKubeConfiguration configuration = Mockito.mock(DirectKubeConfiguration.class);

    private final DefaultPodAffinityFactory factory = new DefaultPodAffinityFactory(configuration);

    @Before
    public void setUp() throws Exception {
        when(configuration.getDefaultGpuInstanceTypes()).thenReturn(Collections.singletonList(DEFAULT_GPU_INSTANCE_TYPE));
    }

    @Test
    public void testInstanceTypeAffinity() {
        V1Affinity affinity = factory.buildV1Affinity(newJobWithHardConstraint(JobConstraints.MACHINE_TYPE, "r5.metal"), JobGenerator.oneBatchTask());
        V1NodeSelector nodeSelector = affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
    }

    @Test
    public void testKubeBackendAffinity() {
        V1Affinity affinity = factory.buildV1Affinity(newJobWithHardConstraint(JobConstraints.KUBE_BACKEND, "kublet"), JobGenerator.oneBatchTask());
        V1NodeSelector nodeSelector = affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        V1NodeSelectorRequirement requirement = nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(requirement.getKey()).isEqualTo(KubeConstants.TAINT_KUBE_BACKEND);
        assertThat(requirement.getValues()).contains("kublet");
    }

    @Test
    public void testEmptyInstanceTypeIsIgnored() {
        V1Affinity affinity = factory.buildV1Affinity(newJobWithHardConstraint(JobConstraints.MACHINE_TYPE, ""), JobGenerator.oneBatchTask());
        assertThat(affinity.getNodeAffinity()).isNull();
    }

    @Test
    public void testDefaultGpuInstanceAssignment() {
        V1Affinity affinity = factory.buildV1Affinity(newGpuJob(Collections.emptyMap()), JobGenerator.oneBatchTask());

        V1NodeSelector nodeSelector = affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(DEFAULT_GPU_INSTANCE_TYPE);
    }

    @Test
    public void testSpecificGpuInstanceAssignment() {
        V1Affinity affinity = factory.buildV1Affinity(newGpuJob(Collections.singletonMap(
                JobConstraints.MACHINE_TYPE, SPECIFIC_GPU_INSTANCE_TYPE
        )), JobGenerator.oneBatchTask());

        V1NodeSelector nodeSelector = affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        assertThat(nodeSelector.getNodeSelectorTerms()).hasSize(1);
        assertThat(nodeSelector.getNodeSelectorTerms().get(0).getMatchExpressions().get(0).getValues().get(0)).isEqualTo(SPECIFIC_GPU_INSTANCE_TYPE);
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