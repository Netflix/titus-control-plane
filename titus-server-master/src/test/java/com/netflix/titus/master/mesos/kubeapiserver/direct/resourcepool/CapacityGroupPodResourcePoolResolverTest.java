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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CapacityGroupPodResourcePoolResolverTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    private final DefaultSettableConfig config = new DefaultSettableConfig();

    private final DirectKubeConfiguration configuration = Archaius2Ext.newConfiguration(DirectKubeConfiguration.class);

    private final ApplicationSlaManagementService capacityGroupService = mock(ApplicationSlaManagementService.class);

    private final CapacityGroupPodResourcePoolResolver resolver = new CapacityGroupPodResourcePoolResolver(
            configuration,
            config,
            capacityGroupService,
            titusRuntime
    );

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

        config.setProperty("elastic", ".*Flex");
        config.setProperty("reserved", ".*Critical");
        clock.advanceTime(1, TimeUnit.HOURS);
    }

    @Test
    public void testBasic() {
        // Map to elastic
        List<ResourcePoolAssignment> result = resolver.resolve(newJob("myFlex"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("elastic");

        // Map to reserved
        result = resolver.resolve(newJob("myCritical"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("reserved");
    }

    @Test
    public void testUpdate() {
        // Entries are ordered so this one gets ahead
        config.setProperty("anElastic", ".*Flex");
        clock.advanceTime(1, TimeUnit.HOURS);

        List<ResourcePoolAssignment> result = resolver.resolve(newJob("myFlex"));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("anElastic");
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