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

package com.netflix.titus.master.mesos;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

@Singleton
public class WorkerStateMonitor {

    private static final Logger logger = LoggerFactory.getLogger(WorkerStateMonitor.class);

    private final VirtualMachineMasterService vmService;
    private AtomicBoolean shutdownFlag = new AtomicBoolean();

    @Inject
    public WorkerStateMonitor(VirtualMachineMasterService vmService,
                              V3JobOperations v3JobOperations,
                              TitusRuntime titusRuntime) {
        this.vmService = vmService;
        vmService.getTaskStatusObservable().subscribe(new Observer<ContainerEvent>() {
            @Override
            public void onCompleted() {
                logger.error("Unexpected end of vmTaskStatusObservable");
            }

            @Override
            public void onError(Throwable e) {
                logger.error("Unknown error from vmTaskStatusObservable - {}", e.getLocalizedMessage());
            }

            @Override
            public void onNext(ContainerEvent containerEvent) {
                try {
                    V3ContainerEvent args = (V3ContainerEvent) containerEvent;
                    if (args.getTaskId() != null) {
                        Optional<Pair<Job<?>, Task>> jobAndTaskOpt = v3JobOperations.findTaskById(args.getTaskId());
                        if (jobAndTaskOpt.isPresent()) {
                            Task task = jobAndTaskOpt.get().getRight();
                            TaskState newState = args.getTaskState();
                            if (task.getStatus().getState() != newState) {

                                String reasonCode = args.getReasonCode();

                                TaskStatus.Builder taskStatusBuilder = JobModel.newTaskStatus()
                                        .withState(newState)
                                        .withTimestamp(args.getTimestamp());

                                // We send kill operation even if task is in Accepted state, but if the latter is the case
                                // we do not want to report Mesos 'lost' state in task status.
                                if (isKillConfirmationForTaskInAcceptedState(task, newState, reasonCode)) {
                                    taskStatusBuilder
                                            .withReasonCode(TaskStatus.REASON_TASK_KILLED)
                                            .withReasonMessage("Task killed before it was launched");
                                } else {
                                    taskStatusBuilder
                                            .withReasonCode(reasonCode)
                                            .withReasonMessage("Mesos task state change event: " + args.getReasonMessage());
                                }
                                TaskStatus taskStatus = taskStatusBuilder.build();

                                // Failures are logged only, as the reconciler will take care of it if needed.
                                final Function<Task, Optional<Task>> updater = JobManagerUtil.newMesosTaskStateUpdater(taskStatus, args.getTitusExecutorDetails(), titusRuntime);
                                v3JobOperations.updateTask(task.getId(), updater, Trigger.Mesos, "Mesos -> " + taskStatus).subscribe(
                                        () -> logger.info("Changed task {} status state to {}", task.getId(), taskStatus),
                                        e -> logger.warn("Could not update task state of {} to {} ({})", args.getTaskId(), taskStatus, e.toString())
                                );
                            }
                            return;
                        }
                    }
                    killOrphanedTask(args);
                } catch (Exception e) {
                    logger.warn("Exception during handling task status update notification", e);
                }
            }
        });
    }

    /**
     * Check if task moved directly from Accepted to KillInitiated.
     */
    private boolean isKillConfirmationForTaskInAcceptedState(Task task, TaskState newState, String reasonCode) {
        if (newState != TaskState.Finished && !TaskStatus.REASON_TASK_LOST.equals(reasonCode)) {
            return false;
        }
        TaskState currentState = task.getStatus().getState();
        if (currentState != TaskState.Accepted && currentState != TaskState.KillInitiated) {
            return false;
        }
        if (task.getStatusHistory().size() > 1) {
            return false;
        }
        if (task.getStatusHistory().isEmpty()) {
            return true;
        }
        TaskState stateInHistory = task.getStatusHistory().get(0).getState();
        return stateInHistory == TaskState.Accepted;
    }

    private void killOrphanedTask(V3ContainerEvent status) {
        String taskId = status.getTaskId();

        // This should never happen, but lets check it anyway
        if (taskId == null) {
            logger.warn("Task status update notification received, but no task id is given: {}", status);
            return;
        }

        // If it is already terminated, do nothing
        if (TaskState.isTerminalState(status.getTaskState())) {
            return;
        }

        logger.warn("Received Mesos callback for unknown task: {} (state {}). Terminating it.", taskId, status.getTaskState());
        vmService.killTask(taskId);
    }

    @PreDestroy
    public void shutdown() {
        shutdownFlag.set(true);
    }
}
