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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TierPodResourcePoolResolverTest {

    private final ApplicationSlaManagementService capacityGroupService = mock(ApplicationSlaManagementService.class);

    private final TierPodResourcePoolResolver resolver = new TierPodResourcePoolResolver(capacityGroupService);

    @Before
    public void setUp() throws Exception {
        when(capacityGroupService.getApplicationSLA("myFlex")).thenReturn(ApplicationSLA.newBuilder()
                .withAppName("myFlex")
                .withTier(Tier.Flex)
                .build()
        );
        when(capacityGroupService.getApplicationSLA("myCritical")).thenReturn(ApplicationSLA.newBuilder()
                .withAppName("myCritical")
                .withTier(Tier.Critical)
                .build()
        );
    }

    @Test
    public void testFlex() {
        List<ResourcePoolAssignment> result = resolver.resolve(newJob("myFlex"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC);
    }

    @Test
    public void testCritical() {
        List<ResourcePoolAssignment> result = resolver.resolve(newJob("myCritical"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_RESERVED);
    }

    private Job newJob(String capacityGroup) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withCapacityGroup(capacityGroup)
                        .build()
                )
                .build();
        return job;
    }
}