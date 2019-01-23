/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.taskmigration.job;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;
import com.netflix.titus.master.taskmigration.TaskMigrationManagerFactory;
import com.netflix.titus.master.taskmigration.V3TaskMigrationDetails;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceJobTaskMigratorTest {

    private final TestScheduler scheduler = Schedulers.test();
    private final TitusRuntime titusRuntime = TitusRuntimes.test(scheduler);

    private final ServiceJobTaskMigratorConfig migratorConfig = mock(ServiceJobTaskMigratorConfig.class);
    private final TaskMigrationManager migrationManager = mock(TaskMigrationManager.class);
    private final TaskMigrationManagerFactory managerFactory = mock(TaskMigrationManagerFactory.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    private final ServiceJobTaskMigrator serviceJobTaskMigrator = new ServiceJobTaskMigrator(scheduler,
            v3JobOperations, migratorConfig, managerFactory, titusRuntime);


    @Test
    public void testMigrateServiceJobs() {

        when(migratorConfig.isServiceTaskMigratorEnabled()).thenReturn(true);
        when(migratorConfig.getSchedulerDelayMs()).thenReturn(1000L);
        when(migratorConfig.getSchedulerTimeoutMs()).thenReturn(300000L);

        TaskMigrationDetails serviceJobOneTaskOne = generateTaskMigrationDetails("Titus-1-worker-0-1", "Titus-1");
        TaskMigrationDetails serviceJobOneTaskTwo = generateTaskMigrationDetails("Titus-1-worker-1-1", "Titus-1");
        TaskMigrationDetails serviceJobTwoTask = generateTaskMigrationDetails("Titus-2-worker-0-1", "Titus-2");
        TaskMigrationDetails serviceJobThreeTask = generateTaskMigrationDetails("Titus-3-worker-0-1", "Titus-3");

        List<TaskMigrationDetails> migrationDetailsList = Lists.newArrayList(serviceJobOneTaskOne, serviceJobOneTaskTwo, serviceJobTwoTask, serviceJobThreeTask);
        migrationDetailsList.forEach(i -> serviceJobTaskMigrator.taskMigrationDetailsMap.put(i.getId(), i));

        when(migrationManager.getState()).thenReturn(TaskMigrationManager.State.Running);

        when(managerFactory.newTaskMigrationManager(any(V3TaskMigrationDetails.class))).thenReturn(migrationManager);

        serviceJobTaskMigrator.enterActiveMode();
        scheduler.advanceTimeBy(0L, TimeUnit.MILLISECONDS);
        verify(migrationManager, times(3)).update(any());
    }

    @Test
    public void testMigrateServiceJobSchedulerTimeout() {

        when(migratorConfig.isServiceTaskMigratorEnabled()).thenReturn(true);
        when(migratorConfig.getSchedulerDelayMs()).thenReturn(1000L);
        when(migratorConfig.getSchedulerTimeoutMs()).thenReturn(300000L);

        TaskMigrationDetails serviceJobOneTask = generateTaskMigrationDetails("Titus-1-worker-0-1", "Titus-1");

        List<TaskMigrationDetails> migrationDetailsList = Collections.singletonList(serviceJobOneTask);
        migrationDetailsList.forEach(i -> serviceJobTaskMigrator.taskMigrationDetailsMap.put(i.getId(), i));

        when(migrationManager.getState()).thenReturn(TaskMigrationManager.State.Running);

        when(managerFactory.newTaskMigrationManager(any(V3TaskMigrationDetails.class))).thenReturn(migrationManager);

        ServiceJobTaskMigrator spy = Mockito.spy(serviceJobTaskMigrator);

        Observable<Void> delay = Observable.timer(1, TimeUnit.HOURS).flatMap(o -> Observable.empty());
        doReturn(delay).when(spy).run();

        spy.enterActiveMode();
        scheduler.advanceTimeBy(400000L, TimeUnit.MILLISECONDS);
        verify(spy, times(2)).run();
    }

    private TaskMigrationDetails generateTaskMigrationDetails(String taskId, String jobId) {
        TaskMigrationDetails taskMigrationDetails = mock(V3TaskMigrationDetails.class);
        when(taskMigrationDetails.getId()).thenReturn(taskId);
        when(taskMigrationDetails.getJobId()).thenReturn(jobId);
        when(taskMigrationDetails.isActive()).thenReturn(true);

        return taskMigrationDetails;
    }
}