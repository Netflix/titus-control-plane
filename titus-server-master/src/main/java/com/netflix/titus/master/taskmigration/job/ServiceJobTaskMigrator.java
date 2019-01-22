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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.fenzo.TaskRequest;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.SchedulerExt;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;
import com.netflix.titus.master.taskmigration.TaskMigrationManagerFactory;
import com.netflix.titus.master.taskmigration.TaskMigrator;
import com.netflix.titus.master.taskmigration.V3TaskMigrationDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;

@Singleton
public class ServiceJobTaskMigrator implements TaskMigrator {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobTaskMigrator.class);

    private final Scheduler scheduler;
    private final V3JobOperations v3JobOperations;
    private final ServiceJobTaskMigratorConfig config;
    private final TaskMigrationManagerFactory managerFactory;

    @VisibleForTesting
    final Map<String, TaskMigrationDetails> taskMigrationDetailsMap;
    private final Map<String, TaskMigrationManager> taskMigrationManagers;
    private final Scheduler.Worker worker;
    private final Registry registry;
    private final TitusRuntime titusRuntime;

    private final Map<String, Gauge> jobsToBeMigratedCounters = new HashMap<>();
    private EnumMap<TaskMigrationManager.State, AtomicInteger> serviceJobsToBeMigrated;
    private Timer runLoopDurationTimer;
    private Set<String> appNamesToIgnore;

    private Action0 action;

    @Inject
    public ServiceJobTaskMigrator(V3JobOperations v3JobOperations,
                                  ServiceJobTaskMigratorConfig config,
                                  TaskMigrationManagerFactory managerFactory,
                                  TitusRuntime titusRuntime) {
        this(SchedulerExt.createSingleThreadScheduler("service-job-task-migrator"),
                v3JobOperations, config, managerFactory, titusRuntime);
    }

    public ServiceJobTaskMigrator(Scheduler scheduler,
                                  V3JobOperations v3JobOperations,
                                  ServiceJobTaskMigratorConfig config,
                                  TaskMigrationManagerFactory managerFactory,
                                  TitusRuntime titusRuntime) {
        this.scheduler = scheduler;
        this.v3JobOperations = v3JobOperations;
        this.config = config;
        this.worker = scheduler.createWorker();
        this.managerFactory = managerFactory;
        this.registry = titusRuntime.getRegistry();
        this.titusRuntime = titusRuntime;

        taskMigrationDetailsMap = new ConcurrentHashMap<>();
        taskMigrationManagers = new ConcurrentHashMap<>();
    }

    @Activator
    public void enterActiveMode() {
        if (action == null) {
            updateAppNamesToIgnore();
            registerMetricMeters();

            action = () -> this.run()
                    .timeout(config.getSchedulerTimeoutMs(), TimeUnit.MILLISECONDS, scheduler)
                    .doOnTerminate(() -> worker.schedule(action, config.getSchedulerDelayMs(), TimeUnit.MILLISECONDS))
                    .subscribe(
                            none -> {
                            },
                            e -> logger.error("Failed to complete the task migration execution with error:", e),
                            () -> logger.debug("Completed the task migration execution")
                    );

            worker.schedule(action);
        }
    }

    @PreDestroy
    public void shutdown() {
        this.worker.unsubscribe();
    }

    @Override
    public void migrate(Collection<TaskRequest> taskRequests) {
        Set<String> taskIds = taskRequests.stream().map(TaskRequest::getId).collect(Collectors.toSet());
        taskMigrationDetailsMap.values().removeIf(taskMigrationDetail -> !taskIds.contains(taskMigrationDetail.getId()));

        for (TaskRequest taskRequest : taskRequests) {
            String taskId = taskRequest.getId();
            try {
                logger.debug("Adding taskId: {} to migration map", taskId);
                logger.debug("Adding v3 taskId: {} to migration map", taskId);
                V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
                Job job = v3QueueableTask.getJob();
                Task task = v3QueueableTask.getTask();
                TaskMigrationDetails taskMigrationDetails = new V3TaskMigrationDetails(job, task, v3JobOperations, titusRuntime);
                if (!appNamesToIgnore.contains(taskMigrationDetails.getApplicationName()) && taskMigrationDetails.isService()) {
                    taskMigrationDetailsMap.putIfAbsent(taskMigrationDetails.getId(), taskMigrationDetails);
                    logger.debug("Added v3 taskId: {} to migration map", taskId);
                }
            } catch (JobManagerException e) {
                if (e.getErrorCode() == JobManagerException.ErrorCode.JobNotFound || e.getErrorCode() == JobManagerException.ErrorCode.TaskNotFound) {
                    logger.info("Task {} already terminated. Migration not needed: {}", taskId, e.getMessage());
                } else {
                    logger.warn("Unable to add taskId: {} to migration map with error:", taskId, e);
                }
            } catch (Exception e) {
                logger.warn("Unable to add taskId: {} to migration map with error:", taskId, e);
            }
        }
    }

    protected Observable<Void> run() {
        if (config.isServiceTaskMigratorEnabled()) {
            return Observable.fromCallable(() -> {
                logger.debug("Starting the task migration execution");
                final long start = registry.clock().wallTime();
                try {
                    updateAppNamesToIgnore();
                    taskMigrationDetailsMap.values().removeIf(taskMigrationDetail -> !taskMigrationDetail.isActive());
                    taskMigrationManagers.values().removeIf(taskMigrationManager -> TaskMigrationManager.isTerminalState(taskMigrationManager.getState()));

                    Map<String, List<TaskMigrationDetails>> tasksPerServiceJob = taskMigrationDetailsMap.values().stream()
                            .collect(Collectors.groupingBy(TaskMigrationDetails::getJobId));

                    tasksPerServiceJob.forEach((jobId, taskMigrationDetailsList) -> {
                        TaskMigrationDetails first = taskMigrationDetailsList.get(0);
                        taskMigrationManagers.computeIfAbsent(jobId, k -> managerFactory.newTaskMigrationManager(first));
                    });

                    taskMigrationManagers.forEach((jobId, taskMigrationManager) -> {
                        List<TaskMigrationDetails> taskMigrationDetailsList = tasksPerServiceJob.getOrDefault(jobId, Collections.emptyList());
                        logger.debug("Updating migration manager for jobId: {} with task size: {}", jobId, taskMigrationDetailsList.size());
                        updateMigrationManager(taskMigrationManager, taskMigrationDetailsList);
                    });
                } catch (Exception e) {
                    logger.error("Unable to execute the run iteration with error: ", e);
                } finally {
                    updateMetricMeters();
                    final long end = registry.clock().wallTime();
                    runLoopDurationTimer.record(end - start, TimeUnit.MILLISECONDS);
                }
                return null;
            });
        } else {
            logger.debug("Task migration is not enabled");
            return Observable.empty();
        }
    }

    private void updateMigrationManager(TaskMigrationManager taskMigrationManager,
                                        List<TaskMigrationDetails> taskMigrationDetailsList) {
        try {
            long deadline = System.currentTimeMillis() + taskMigrationManager.getTimeoutMs();
            for (TaskMigrationDetails taskMigrationDetails : taskMigrationDetailsList) {
                taskMigrationDetails.setMigrationDeadline(deadline);
            }
            taskMigrationManager.update(taskMigrationDetailsList);
        } catch (Exception e) {
            logger.error("Unable to migrate service job with error: ", e);
        }
    }

    private void registerMetricMeters() {
        serviceJobsToBeMigrated = new EnumMap<>(TaskMigrationManager.State.class);
        for (TaskMigrationManager.State state : TaskMigrationManager.State.values()) {
            Id id = registry.createId(MetricConstants.METRIC_TASK_MIGRATION + "serviceJobsToBeMigrated", "state", state.name().toLowerCase());
            AtomicInteger atomicInteger = registry.gauge(id, new AtomicInteger(0));
            serviceJobsToBeMigrated.put(state, atomicInteger);
        }

        runLoopDurationTimer = registry.timer(MetricConstants.METRIC_TASK_MIGRATION + "runLoopDurationTimer");
    }

    private void updateMetricMeters() {
        Map<String, Integer> tasksToBeMigratedPerJob = new HashMap<>();
        Map<String, String> jobToApplicationNameMap = new HashMap<>();
        taskMigrationDetailsMap.forEach((id, migrationDetails) ->
                {
                    tasksToBeMigratedPerJob.put(
                            migrationDetails.getJobId(),
                            tasksToBeMigratedPerJob.getOrDefault(migrationDetails.getJobId(), 0) + 1
                    );
                    jobToApplicationNameMap.put(migrationDetails.getJobId(), migrationDetails.getApplicationName());
                }
        );

        Set<String> jobCountersToClear = CollectionsExt.copyAndRemove(jobsToBeMigratedCounters.keySet(), tasksToBeMigratedPerJob.keySet());
        jobCountersToClear.forEach(jobId -> jobsToBeMigratedCounters.remove(jobId).set(0));

        tasksToBeMigratedPerJob.forEach((jobId, counter) ->
                jobsToBeMigratedCounters.computeIfAbsent(jobId, jid ->
                        registry.gauge(MetricConstants.METRIC_TASK_MIGRATION + "tasksToBeMigrated",
                                "jobId", jobId,
                                "applicationName", jobToApplicationNameMap.getOrDefault(jobId, "UNKNOWN")
                        )
                ).set(counter)
        );

        Map<TaskMigrationManager.State, Long> countPerState = taskMigrationManagers.values().stream()
                .collect(Collectors.groupingBy(TaskMigrationManager::getState, Collectors.counting()));

        for (TaskMigrationManager.State state : TaskMigrationManager.State.values()) {
            Long count = countPerState.getOrDefault(state, 0L);
            AtomicInteger atomicInteger = serviceJobsToBeMigrated.get(state);
            atomicInteger.set(count.intValue());
        }
    }

    private void updateAppNamesToIgnore() {
        appNamesToIgnore = new HashSet<>(StringExt.splitByComma(config.getAppNamesToIgnore()));
    }
}
