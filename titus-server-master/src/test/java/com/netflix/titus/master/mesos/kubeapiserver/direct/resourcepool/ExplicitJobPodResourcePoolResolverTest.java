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

package com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExplicitJobPodResourcePoolResolverTest {

    private final ExplicitJobPodResourcePoolResolver resolver = new ExplicitJobPodResourcePoolResolver();

    @Test
    public void testAssignment() {
        List<ResourcePoolAssignment> result = resolver.resolve(newJob("myResourcePool"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("myResourcePool");
    }

    private Job newJob(String resourcePool) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withAttributes(Collections.singletonMap(
                                JobAttributes.JOB_PARAMETER_RESOURCE_POOLS,
                                resourcePool
                        ))
                        .build()
                )
                .build();
        return job;
    }

}