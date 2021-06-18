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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.List;

import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GpuPodResourcePoolResolverTest {

    private final DefaultSettableConfig config = new DefaultSettableConfig();

    private final KubePodConfiguration configuration = Archaius2Ext.newConfiguration(KubePodConfiguration.class, config);

    private final ApplicationSlaManagementService capacityGroupService = mock(ApplicationSlaManagementService.class);

    private GpuPodResourcePoolResolver resolver;

    private final Task task = JobGenerator.oneBatchTask();

    @Before
    public void setUp() {
        config.setProperty("titusMaster.kubernetes.pod.gpuResourcePoolNames", "gpu1,gpu2");
        resolver = new GpuPodResourcePoolResolver(configuration, capacityGroupService);
    }

    @Test
    public void testNonGpuJob() {
        assertThat(resolver.resolve(newJob(0, null), task)).isEmpty();
    }

    @Test
    public void testGpuJob() {
        config.setProperty("titusMaster.kubernetes.pod.gpuResourcePoolNames", "gpu1,gpu2");
        List<ResourcePoolAssignment> result = resolver.resolve(newJob(1, null), task);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("gpu1");
        assertThat(result.get(1).getResourcePoolName()).isEqualTo("gpu2");
    }

    private Job<?> newJob(int gpuCount, String capacityGroup) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        return job.toBuilder().withJobDescriptor(
                job.getJobDescriptor().toBuilder()
                        .withCapacityGroup(capacityGroup)
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withContainerResources(
                                        ContainerResources.newBuilder().withGpu(gpuCount).build()
                                )
                                .build()
                        )
                        .build()
        ).build();
    }

    @Test
    // Validate that when the job is using a Capacity Group with a GPU resource pool, we resolve to
    // exactly that one resource pool
    public void testGpuJobWithGpuCapacityGroup() {
        when(capacityGroupService.getApplicationSLA("gpu1_capacity_group")).thenReturn(
                ApplicationSLA.newBuilder()
                        .withAppName("gpu1_capacity_group")
                        .withTier(Tier.Flex)
                        .withResourcePool("gpu1")
                        .build()
        );
        Job<?> job = newJob(1, "gpu1_capacity_group");
        List<ResourcePoolAssignment> result = resolver.resolve(job, task);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("gpu1");
    }

    @Test
    // Validate despite specifying a Capacity Group with a non-GPU resource pool, we get the resolution
    // to a pre-configured GPU resource pools list
    public void testGpuJobWithElasticCapacityGroup() {
        when(capacityGroupService.getApplicationSLA("cg_with_elastic_resourcepool")).thenReturn(
                ApplicationSLA.newBuilder()
                        .withAppName("cg_with_elastic_resourcepool")
                        .withTier(Tier.Flex)
                        .withResourcePool("elastic")
                        .build()
        );
        Job<?> job = newJob(1, "cg_with_elastic_resourcepool");
        List<ResourcePoolAssignment> result = resolver.resolve(job, task);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("gpu1");
        assertThat(result.get(1).getResourcePoolName()).isEqualTo("gpu2");
    }
}