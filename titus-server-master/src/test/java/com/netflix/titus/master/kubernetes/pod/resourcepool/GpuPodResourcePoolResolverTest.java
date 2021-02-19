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
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GpuPodResourcePoolResolverTest {

    private final DefaultSettableConfig config = new DefaultSettableConfig();

    private final KubePodConfiguration configuration = Archaius2Ext.newConfiguration(KubePodConfiguration.class, config);

    private final GpuPodResourcePoolResolver resolver = new GpuPodResourcePoolResolver(configuration);

    @Test
    public void testNonGpuJob() {
        assertThat(resolver.resolve(newJob(0))).isEmpty();
    }

    @Test
    public void testGpuJob() {
        config.setProperty("titusMaster.kubernetes.pod.gpuResourcePoolNames", "gpu1,gpu2");
        List<ResourcePoolAssignment> result = resolver.resolve(newJob(1));
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("gpu1");
        assertThat(result.get(1).getResourcePoolName()).isEqualTo("gpu2");
    }

    private Job<?> newJob(int gpuCount) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        return job.toBuilder().withJobDescriptor(
                job.getJobDescriptor().toBuilder()
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withContainerResources(
                                        ContainerResources.newBuilder().withGpu(gpuCount).build()
                                )
                                .build()
                        )
                        .build()
        ).build();
    }
}