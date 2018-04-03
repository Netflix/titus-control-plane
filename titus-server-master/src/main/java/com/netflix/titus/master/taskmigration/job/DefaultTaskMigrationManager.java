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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.store.InvalidJobStateChangeException;
import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;
import com.netflix.titus.master.taskmigration.V2TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.V3TaskMigrationDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTaskMigrationManager implements TaskMigrationManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultTaskMigrationManager.class);
    private static final int KILL_TIMEOUT = 10;

    private final ServiceJobTaskMigratorConfig config;
    private final TokenBucket terminateTokenBucket;

    private State state;
    private long lastMovedWorkerOnDisabledVM;

    public DefaultTaskMigrationManager(ServiceJobTaskMigratorConfig config, TokenBucket terminateTokenBucket) {
        this.config = config;
        this.terminateTokenBucket = terminateTokenBucket;
        this.state = State.Pending;
    }

    @Override
    public void update(Collection<TaskMigrationDetails> taskMigrationDetailsCollection) {
        logger.debug("Starting update");
        if (taskMigrationDetailsCollection.isEmpty()) {
            state = State.Succeeded;
        } else {
            state = State.Running;

            TaskMigrationDetails first = taskMigrationDetailsCollection.iterator().next();
            String jobId = first.getJobId();

            if (lastMovedWorkerOnDisabledVM > (System.currentTimeMillis() - config.getMigrateIntervalMs())) {
                logger.debug("Skipping iteration due to throttle for jobId: {}", jobId);
                return; // Throttle how often we migrate
            }

            List<V2TaskMigrationDetails> v2TaskMigrationDetailsCollections = new ArrayList<>();
            List<V3TaskMigrationDetails> v3TaskMigrationDetailsCollections = new ArrayList<>();
            for (TaskMigrationDetails taskMigrationDetails : taskMigrationDetailsCollection) {
                if (taskMigrationDetails instanceof V2TaskMigrationDetails) {
                    v2TaskMigrationDetailsCollections.add((V2TaskMigrationDetails) taskMigrationDetails);
                } else if (taskMigrationDetails instanceof V3TaskMigrationDetails) {
                    v3TaskMigrationDetailsCollections.add((V3TaskMigrationDetails) taskMigrationDetails);
                }
            }

            if (!v2TaskMigrationDetailsCollections.isEmpty()) {
                migrateV2Tasks(v2TaskMigrationDetailsCollections);
            } else if (!v3TaskMigrationDetailsCollections.isEmpty()) {
                migrateV3Tasks(v3TaskMigrationDetailsCollections);
            }
        }
        logger.debug("Finishing update");
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public long getTimeoutMs() {
        return 0;
    }

    private void migrateV2Tasks(Collection<V2TaskMigrationDetails> taskMigrationDetailsCollection) {
        V2TaskMigrationDetails first = taskMigrationDetailsCollection.iterator().next();
        String jobId = first.getJobId();
        V2JobMgrIntf jobManager = first.getJobManager();
        try (AutoCloseable ignored = jobManager.getJobMetadata().obtainLock()) {
            int numberOfInstances = first.getNumberOfInstances();
            long numberOfRunningWorkers = jobManager.getWorkers().stream()
                    .filter(t -> V2JobState.isRunningState(t.getState())).count();

            if (numberOfRunningWorkers != numberOfInstances) {
                logger.debug("Skipping iteration for jobId: {} because worker size: {} does not match number of instances: {}",
                        jobId, numberOfRunningWorkers, numberOfInstances);
                return; // Only migrate when the desired number of workers are running
            }

            // calculate how many tasks to migrate by taking the percent in the config, converting it into a decimal,
            // and multiplying the desired by the decimal.
            long numWorkersToMove = Math.round((double) numberOfInstances * (config.getIterationPercent() / 100));
            LinkedList<V2WorkerMetadata> workersToMove = new LinkedList<>();

            for (V2TaskMigrationDetails taskMigrationDetails : taskMigrationDetailsCollection) {
                V2WorkerMetadata workerMetadata = taskMigrationDetails.getWorkerMetadata();
                if (workerMetadata == null) {
                    logger.debug("Skipping iteration for jobId: {} because workerMetadata is null");
                    continue; // Worker is not active, ignore it
                }

                workersToMove.addLast(workerMetadata);
                if (workersToMove.size() >= numWorkersToMove) {
                    break; // We have enough workers to move
                }
            }
            logger.debug("Attempting to move {} workers for jobId: {}", workersToMove.size(), jobId);

            for (V2WorkerMetadata workerMetadata : workersToMove) {
                if (V2JobState.isRunningState(workerMetadata.getState())) {
                    try {
                        if (terminateTokenBucket.tryTake()) {
                            logger.info("Migrating worker: {} and index: {} of job: {}", workerMetadata.getWorkerNumber(), workerMetadata.getWorkerIndex(), first.getJobId());
                            jobManager.resubmitWorker(
                                    WorkerNaming.getWorkerName(workerMetadata.getJobId(), workerMetadata.getWorkerIndex(),
                                            workerMetadata.getWorkerNumber()),
                                    "Moving service task index " + workerMetadata.getWorkerIndex() + ", number " +
                                            workerMetadata.getWorkerNumber() + " out of disabled VM");
                            lastMovedWorkerOnDisabledVM = System.currentTimeMillis();
                        }
                    } catch (InvalidJobStateChangeException | InvalidJobException e) {
                        logger.warn("No such worker: {} of job: {} to resubmit due to the error:", workerMetadata.getWorkerNumber(), first.getJobId(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Unable to migrate tasks for jobId: {} with error:", first.getJobId(), e);
        }
    }

    private void migrateV3Tasks(Collection<V3TaskMigrationDetails> taskMigrationDetailsCollection) {
        V3TaskMigrationDetails first = taskMigrationDetailsCollection.iterator().next();
        V3JobOperations v3JobOperations = first.getV3JobOperations();
        String jobId = first.getJobId();
        try {
            int numberOfInstances = first.getNumberOfInstances();
            ServiceJobExt extensions = (ServiceJobExt) first.getJob().getJobDescriptor().getExtensions();
            Capacity capacity = extensions.getCapacity();

            if (numberOfInstances != capacity.getDesired()) {
                logger.debug("Skipping iteration for jobId: {} because number of instances: {} does not match desired: {}",
                        jobId, numberOfInstances, capacity.getDesired());
                return; // Only migrate when the desired number of instances are running
            }
            long numberOfInstancesToMove = Math.round((double) numberOfInstances * (config.getIterationPercent() / 100));
            LinkedList<Task> tasksToMove = new LinkedList<>();

            for (V3TaskMigrationDetails taskMigrationDetails : taskMigrationDetailsCollection) {
                tasksToMove.addLast(taskMigrationDetails.getTask());
                if (tasksToMove.size() >= numberOfInstancesToMove) {
                    break; // We have enough tasks to move
                }
            }
            logger.debug("Attempting to move {} tasks for jobId: {}", tasksToMove.size(), jobId);

            for (Task task : tasksToMove) {
                TaskState state = task.getStatus().getState();
                if (state == TaskState.Launched || state == TaskState.StartInitiated || state == TaskState.Started) {
                    if (terminateTokenBucket.tryTake()) {
                        logger.info("Migrating task: {} of job: {}", task.getId(), jobId);
                        String reason = "Moving service task: " + task.getId() + " out of disabled VM";
                        try {
                            v3JobOperations.killTask(task.getId(), false, reason).toCompletable().await(KILL_TIMEOUT, TimeUnit.MILLISECONDS);
                            lastMovedWorkerOnDisabledVM = System.currentTimeMillis();
                        } catch (Exception e) {
                            logger.error("Unable to kill task: {} with error: ", task.getId(), e);
                        }
                    }
                }
            }
        } catch (JobManagerException e) {
            if (e.getErrorCode() == JobManagerException.ErrorCode.JobNotFound || e.getErrorCode() == JobManagerException.ErrorCode.TaskNotFound) {
                logger.info("Job/task already terminated. Migration not needed: {}", e.getMessage());
            } else {
                logger.error("Unable to migrate tasks for jobId: {} with error:", jobId, e);
            }
        } catch (Exception e) {
            logger.error("Unable to migrate tasks for jobId: {} with error:", jobId, e);
        }
    }
}
