/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.taskmigration.job;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
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
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.scheduler.ScheduledRequest;
import io.netflix.titus.master.taskmigration.TaskMigrationDetails;
import io.netflix.titus.master.taskmigration.TaskMigrationManager;
import io.netflix.titus.master.taskmigration.TaskMigrationManagerFactory;
import io.netflix.titus.master.taskmigration.TaskMigrator;
import io.netflix.titus.master.taskmigration.V2TaskMigrationDetails;
import io.netflix.titus.master.taskmigration.V3TaskMigrationDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.schedulers.Schedulers;

@Singleton
public class ServiceJobTaskMigrator implements TaskMigrator {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobTaskMigrator.class);

    private final Scheduler scheduler;
    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;
    private final ServiceJobTaskMigratorConfig config;
    private final TaskMigrationManagerFactory managerFactory;

    @VisibleForTesting
    final Map<String, TaskMigrationDetails> taskMigrationDetailsMap;
    private final Map<String, TaskMigrationManager> taskMigrationManagers;
    private final Scheduler.Worker worker;
    private final Registry registry;

    private AtomicInteger tasksToBeMigrated;
    private EnumMap<TaskMigrationManager.State, AtomicInteger> serviceJobsToBeMigrated;
    private Timer runLoopDurationTimer;
    private Set<String> appNamesToIgnore;

    private Action0 action;

    @Inject
    public ServiceJobTaskMigrator(V2JobOperations v2JobOperations,
                                  V3JobOperations v3JobOperations,
                                  ServiceJobTaskMigratorConfig config,
                                  TaskMigrationManagerFactory managerFactory,
                                  Registry registry) {
        this(Schedulers.newThread(), v2JobOperations, v3JobOperations, config, managerFactory, registry);
    }

    public ServiceJobTaskMigrator(Scheduler scheduler,
                                  V2JobOperations v2JobOperations,
                                  V3JobOperations v3JobOperations,
                                  ServiceJobTaskMigratorConfig config,
                                  TaskMigrationManagerFactory managerFactory,
                                  Registry registry) {
        this.scheduler = scheduler;
        this.v2JobOperations = v2JobOperations;
        this.v3JobOperations = v3JobOperations;
        this.config = config;
        this.worker = scheduler.createWorker();
        this.managerFactory = managerFactory;
        this.registry = registry;

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
            logger.debug("Adding taskRequest: {} to migration map", taskRequest.getId());
            if (taskRequest instanceof ScheduledRequest) {
                logger.debug("Adding v2 taskRequest: {} to migration map", taskRequest.getId());
                WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskRequest.getId());
                V2JobMgrIntf jobManager = v2JobOperations.getJobMgr(jobAndWorkerId.jobId);

                if (jobManager != null) {
                    TaskMigrationDetails taskMigrationDetails = new V2TaskMigrationDetails(taskRequest, jobManager);
                    if (!appNamesToIgnore.contains(taskMigrationDetails.getApplicationName()) && taskMigrationDetails.isService()) {
                        taskMigrationDetailsMap.putIfAbsent(taskMigrationDetails.getId(), taskMigrationDetails);
                    }
                }
            } else if (taskRequest instanceof V3QueueableTask) {
                logger.debug("Adding v3 taskRequest: {} to migration map", taskRequest.getId());
                V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
                Job job = v3QueueableTask.getJob();
                Task task = v3QueueableTask.getTask();
                TaskMigrationDetails taskMigrationDetails = new V3TaskMigrationDetails(job, task, v3JobOperations);
                if (!appNamesToIgnore.contains(taskMigrationDetails.getApplicationName()) && taskMigrationDetails.isService()) {
                    taskMigrationDetailsMap.putIfAbsent(taskMigrationDetails.getId(), taskMigrationDetails);
                }
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
        Id tasksToBeMigratedId = registry.createId(MetricConstants.METRIC_TASK_MIGRATION + "tasksToBeMigrated");
        tasksToBeMigrated = registry.gauge(tasksToBeMigratedId, new AtomicInteger(0));

        serviceJobsToBeMigrated = new EnumMap<>(TaskMigrationManager.State.class);
        for (TaskMigrationManager.State state : TaskMigrationManager.State.values()) {
            Id id = registry.createId(MetricConstants.METRIC_TASK_MIGRATION + "serviceJobsToBeMigrated", "state", state.name().toLowerCase());
            AtomicInteger atomicInteger = registry.gauge(id, new AtomicInteger(0));
            serviceJobsToBeMigrated.put(state, atomicInteger);
        }

        runLoopDurationTimer = registry.timer(MetricConstants.METRIC_TASK_MIGRATION + "runLoopDurationTimer");
    }

    private void updateMetricMeters() {
        tasksToBeMigrated.set(taskMigrationDetailsMap.size());

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
