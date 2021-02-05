/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.store;

import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArchivedTasksGcTest {

    private final ArchivedTasksGcConfiguration configuration = mock(ArchivedTasksGcConfiguration.class);
    private final V3JobOperations jobOperations = mock(V3JobOperations.class);
    private final JobStore jobStore = mock(JobStore.class);
    private final TitusRuntime titusRuntime = TitusRuntimes.test();
    private final ArchivedTasksGc archivedTasksGc = new ArchivedTasksGc(configuration, jobOperations, jobStore, titusRuntime);

    @BeforeEach
    void setUp() {
        when(configuration.isGcEnabled()).thenReturn(true);
        when(configuration.getGcInitialDelayMs()).thenReturn(1L);
        when(configuration.getGcIntervalMs()).thenReturn(1L);
        when(configuration.getGcTimeoutMs()).thenReturn(1000000L);
        when(configuration.getMaxNumberOfArchivedTasksPerJob()).thenReturn(1L);
        when(configuration.getMaxNumberOfArchivedTasksToGcPerIteration()).thenReturn(1);
    }

    @Test
    public void gcArchivedTasks() {
        when(configuration.isGcEnabled()).thenReturn(true);
    }

    @Test
    public void gc() {
    }

    private List<Job<ServiceJobExt>> createServiceJobs(int n) {
        List<Job<ServiceJobExt>> serviceJobs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            serviceJobs.add(createServiceJob());
        }
        return serviceJobs;
    }

    private Job<ServiceJobExt> createServiceJob() {
        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor().but(jd ->
                jd.getExtensions().toBuilder().build()
        );
        return JobGenerator.serviceJobs(jobDescriptor).getValue();
    }
}