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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArchivedTasksGcTest {

    private final ArchivedTasksGcConfiguration configuration = mock(ArchivedTasksGcConfiguration.class);
    private final V3JobOperations jobOperations = mock(V3JobOperations.class);
    private final StubbedJobStore jobStore = new StubbedJobStore();
    private final TitusRuntime titusRuntime = TitusRuntimes.test();
    private ArchivedTasksGc archivedTasksGc;

    @Before
    public void setUp() {
        when(configuration.isGcEnabled()).thenReturn(true);
        when(configuration.getGcInitialDelayMs()).thenReturn(1L);
        when(configuration.getGcIntervalMs()).thenReturn(1L);
        when(configuration.getGcTimeoutMs()).thenReturn(1000000L);
        when(configuration.getMaxNumberOfArchivedTasksPerJob()).thenReturn(5L);
        when(configuration.getMaxNumberOfArchivedTasksToGcPerIteration()).thenReturn(2);
        when(configuration.getMaxRxConcurrency()).thenReturn(1);

        archivedTasksGc = new ArchivedTasksGc(configuration, jobOperations, jobStore, titusRuntime);
    }

    @Test
    public void gcArchivedTasks() {
        when(configuration.isGcEnabled()).thenReturn(true);
    }

    @Test
    public void gc() {
        Job<ServiceJobExt> job = createServiceJob(10);

        // Limit is max 2 tasks per iteration per job
        archivedTasksGc.gc();
        assertThat(jobStore.getArchivedTasksInternal(job.getId())).hasSize(8);
        archivedTasksGc.gc();
        assertThat(jobStore.getArchivedTasksInternal(job.getId())).hasSize(6);
        archivedTasksGc.gc();
        assertThat(jobStore.getArchivedTasksInternal(job.getId())).hasSize(5);
        archivedTasksGc.gc();
        assertThat(jobStore.getArchivedTasksInternal(job.getId())).hasSize(5);
    }

    private Job<ServiceJobExt> createServiceJob(int archivedTasksCount) {
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        jobStore.storeJob(job).get();
        for (int i = 0; i < archivedTasksCount; i++) {
            Task task = JobGenerator.serviceTasks(job).getValue().toBuilder().withId("task" + i).build();
            task = JobFunctions.changeTaskStatus(task, TaskStatus.newBuilder().withState(TaskState.Finished).build());
            jobStore.addArchivedTaskInternal(task);
        }

        when(jobOperations.getJobs()).thenReturn(new ArrayList<>(jobStore.getJobsInternal().values()));

        return job;
    }
}