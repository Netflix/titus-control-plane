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

package com.netflix.titus.master.kubernetes.pod.taint;

import java.util.List;

import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Toleration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DefaultTaintTolerationFactoryTest {

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    private final ApplicationSlaManagementService capacityManagement = mock(ApplicationSlaManagementService.class);

    private final DefaultTaintTolerationFactory factory = new DefaultTaintTolerationFactory(
            configuration,
            capacityManagement
    );

    @Test
    public void decommissioningNodesAreTolerated() {
        List<V1Toleration> tolerations = factory.buildV1Toleration(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        assertThat(tolerations).contains(Tolerations.TOLERATION_DECOMMISSIONING);

        List<V1Toleration> withConstraints = factory.buildV1Toleration(newJobWithConstraint(JobConstraints.ACTIVE_HOST, "true"), JobGenerator.oneBatchTask());
        assertThat(withConstraints).doesNotContain(Tolerations.TOLERATION_DECOMMISSIONING);
    }

    @Test
    public void testGpuInstanceAssignment() {
        List<V1Toleration> tolerations = factory.buildV1Toleration(newGpuJob(), JobGenerator.oneBatchTask());
        V1Toleration gpuToleration = tolerations.stream().filter(t -> t.getKey().equals(KubeConstants.TAINT_GPU_INSTANCE)).findFirst().orElse(null);
        assertThat(gpuToleration).isEqualTo(Tolerations.TOLERATION_GPU_INSTANCE);
    }

    @Test
    public void testKubeBackendToleration() {
        List<V1Toleration> tolerations = factory.buildV1Toleration(newJobWithConstraint(JobConstraints.KUBE_BACKEND, "kublet"), JobGenerator.oneBatchTask());
        V1Toleration gpuToleration = tolerations.stream().filter(t -> t.getKey().equals(KubeConstants.TAINT_KUBE_BACKEND)).findFirst().orElse(null);
        assertThat(gpuToleration.getKey()).isEqualTo(KubeConstants.TAINT_KUBE_BACKEND);
        assertThat(gpuToleration.getValue()).isEqualTo("kublet");
    }

    private Job newJobWithConstraint(String name, String value) {
        return JobFunctions.appendHardConstraint(JobGenerator.oneBatchJob(), name, value);
    }

    private Job<BatchJobExt> newGpuJob() {
        Job<BatchJobExt> template = JobGenerator.oneBatchJob();
        JobDescriptor<BatchJobExt> jobDescriptor = template.getJobDescriptor();
        Container container = jobDescriptor.getContainer();

        return template.toBuilder()
                .withJobDescriptor(jobDescriptor.toBuilder()
                        .withContainer(container.toBuilder()
                                .withContainerResources(container.getContainerResources().toBuilder().withGpu(1).build())
                                .build()
                        )
                        .build()
                )
                .build();
    }
}