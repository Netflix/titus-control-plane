/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.Arrays;
import java.util.Map;

import com.netflix.titus.api.endpoint.v2.rest.representation.ReservationUsage;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReservationUsageCalculatorTest {

    private static final JobDescriptor<BatchJobExt> JOB_DESCRIPTOR = oneTaskBatchJobDescriptor().but(JobFunctions.ofBatchSize(2));

    private static final ContainerResources CONTAINER_RESOURCES = JOB_DESCRIPTOR.getContainer().getContainerResources();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private ApplicationSlaManagementService capacityManagementService = mock(ApplicationSlaManagementService.class);

    private final ReservationUsageCalculator calculator = new ReservationUsageCalculator(
            jobComponentStub.getJobOperations(),
            capacityManagementService
    );

    @Before
    public void setUp() throws Exception {
        when(capacityManagementService.getApplicationSLAs()).thenReturn(Arrays.asList(
                ApplicationSLA.newBuilder().withAppName("cg1").build(),
                ApplicationSLA.newBuilder().withAppName("cg2").build()
        ));
        when(capacityManagementService.getApplicationSLA("cg1")).thenReturn(
                ApplicationSLA.newBuilder().withAppName("cg1").build()
        );

        addJobsInCapacityGroup("cg1", 2);
        addJobsInCapacityGroup("cg2", 4);
    }

    @Test
    public void testBuildUsage() {
        Map<String, ReservationUsage> usage = calculator.buildUsage();

        ReservationUsage cg1Usage = usage.get("cg1");
        ReservationUsage cg2Usage = usage.get("cg2");

        assertThat(cg1Usage).isNotNull();
        assertThat(cg2Usage).isNotNull();

        assertThat(cg1Usage.getCpu()).isEqualTo(CONTAINER_RESOURCES.getCpu() * 4);
        assertThat(cg1Usage.getCpu() * 2).isEqualTo(cg2Usage.getCpu());

        assertThat(cg1Usage.getMemoryMB()).isEqualTo(CONTAINER_RESOURCES.getMemoryMB() * 4);
        assertThat(cg1Usage.getMemoryMB() * 2).isEqualTo(cg2Usage.getMemoryMB());

        assertThat(cg1Usage.getDiskMB()).isEqualTo(CONTAINER_RESOURCES.getDiskMB() * 4);
        assertThat(cg1Usage.getDiskMB() * 2).isEqualTo(cg2Usage.getDiskMB());

        assertThat(cg1Usage.getNetworkMbs()).isEqualTo(CONTAINER_RESOURCES.getNetworkMbps() * 4);
        assertThat(cg1Usage.getNetworkMbs() * 2).isEqualTo(cg2Usage.getNetworkMbs());
    }

    @Test
    public void testBuildCapacityGroupUsage() {
        ReservationUsage cg1Usage = calculator.buildCapacityGroupUsage("cg1");

        assertThat(cg1Usage).isNotNull();
        assertThat(cg1Usage.getCpu()).isEqualTo(CONTAINER_RESOURCES.getCpu() * 4);
        assertThat(cg1Usage.getMemoryMB()).isEqualTo(CONTAINER_RESOURCES.getMemoryMB() * 4);
        assertThat(cg1Usage.getDiskMB()).isEqualTo(CONTAINER_RESOURCES.getDiskMB() * 4);
        assertThat(cg1Usage.getNetworkMbs()).isEqualTo(CONTAINER_RESOURCES.getNetworkMbps() * 4);
    }

    private void addJobsInCapacityGroup(String capacityGroup, int jobCount) {
        DataGenerator<Job<BatchJobExt>> jobGenerator = JobGenerator.batchJobs(JOB_DESCRIPTOR

                .but(jd -> jd.toBuilder().withCapacityGroup(capacityGroup))
        );
        jobGenerator.getValues(jobCount).forEach(job -> {
            jobComponentStub.createJob(job);
            jobComponentStub.createDesiredTasks(job);
            jobComponentStub.getJobOperations().getTasks(job.getId()).forEach(task -> jobComponentStub.moveTaskToState(task, TaskState.Started));
        });
    }
}