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
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;


public class MixedSchedulerResourcePoolTest {

    private final Task task = JobGenerator.oneBatchTask();

    private final DefaultSettableConfig config = new DefaultSettableConfig();
    private final KubePodConfiguration configuration = Archaius2Ext.newConfiguration(KubePodConfiguration.class, config);
    private MixedSchedulerResourcePoolResolver resolver;

    @Before
    public void setUp() throws Exception {
        config.setProperty("titusMaster.kubernetes.pod.mixedSchedulingEnabled", "true");
        resolver = new MixedSchedulerResourcePoolResolver(configuration);
    }

    @Test
    public void testCPUHeavyJobGetsBothPoolsAndPrefersReserved() {
        List<ResourcePoolAssignment> result = resolver.resolve(newCPUJob(), task);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC);
        assertThat(result.get(0).preferred()).isFalse();
        assertThat(result.get(1).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_RESERVED);
        assertThat(result.get(1).preferred()).isTrue();
    }

    @Test
    public void testRamHeavyJobGetsBothPoolsAndPrefersElastic() {
        List<ResourcePoolAssignment> result = resolver.resolve(newRamJob(), task);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC);
        assertThat(result.get(0).preferred()).isTrue();
        assertThat(result.get(1).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_RESERVED);
        assertThat(result.get(1).preferred()).isFalse();
    }

    private Job newCPUJob() {
        JobDescriptor<BatchJobExt> j = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().toBuilder()
                .withContainerResources(ContainerResources.newBuilder()
                        .withCpu(100)
                        .withMemoryMB(1_000)
                        .build()
                )
                .build()
        );
        return JobGenerator.oneBatchJob().toBuilder()
                .withJobDescriptor(j).build();
    }

    private Job newRamJob() {
        JobDescriptor<BatchJobExt> j = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().toBuilder()
                .withContainerResources(ContainerResources.newBuilder()
                        .withCpu(1)
                        .withMemoryMB(472_000)
                        .build()
                )
                .build()
        );
        return JobGenerator.oneBatchJob().toBuilder()
                .withJobDescriptor(j).build();
    }
}