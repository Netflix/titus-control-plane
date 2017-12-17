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

package io.netflix.titus.master.job.worker.internal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.job.JobManagerConfiguration;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.job.worker.WorkerStateMonitor;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

@Singleton
public class DefaultWorkerStateMonitor implements WorkerStateMonitor {

    private class StateToMonitor {
        private final V2JobMgrIntf jobMgr;
        private int workerIndex;
        private int workerNumber;
        private final String taskId;
        private V2JobState state;

        StateToMonitor(V2JobMgrIntf jobMgr, int workerIndex, int workerNumber, String taskId, V2JobState state) {
            this.jobMgr = jobMgr;
            this.workerIndex = workerIndex;
            this.workerNumber = workerNumber;
            this.taskId = taskId;
            this.state = state;
        }

        @Override
        public String toString() {
            return jobMgr.getJobId() + "-worker-" + workerIndex + "-" + workerNumber + ": state " + state;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultWorkerStateMonitor.class);
    private final VirtualMachineMasterService vmService;
    private final V2JobOperations jobOps;
    private final Observable<V2JobMgrIntf> jobCreationObservable;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final PublishSubject<StateToMonitor> workerStatesSubject;
    private final PublishSubject<Status> allStatusSubject;
    private AtomicBoolean shutdownFlag = new AtomicBoolean();

    @Inject
    public DefaultWorkerStateMonitor(VirtualMachineMasterService vmService,
                                     V2JobOperations jOps,
                                     V3JobOperations v3JobOperations,
                                     JobManagerConfiguration jobManagerConfiguration) {
        this.vmService = vmService;
        this.jobOps = jOps;
        this.jobCreationObservable = jOps.getJobCreationPublishSubject();
        this.jobManagerConfiguration = jobManagerConfiguration;
        workerStatesSubject = PublishSubject.create();
        workerStatesSubject
                .groupBy(stateToMonitor ->
                        stateToMonitor.jobMgr.getJobId() + stateToMonitor.workerIndex + "-" + stateToMonitor.workerNumber
                )
                .flatMap(statesGO -> statesGO
                        .debounce(2000, TimeUnit.MILLISECONDS)
                        .map(stateToMonitor -> {
                            Schedulers.computation().createWorker().schedule(() ->
                                    handleJobStuck(stateToMonitor), getMillisToWait(stateToMonitor.state, stateToMonitor.jobMgr), TimeUnit.MILLISECONDS
                            );
                            return !V2JobState.isTerminalState(stateToMonitor.state);
                        })
                        .takeWhile(keepGoing -> keepGoing))
                .subscribe();
        vmService.getTaskStatusObservable().subscribe(new Observer<Status>() {
            @Override
            public void onCompleted() {
                logger.error("Unexpected end of vmTaskStatusObservable");
            }

            @Override
            public void onError(Throwable e) {
                logger.error("Unknown error from vmTaskStatusObservable - " + e.getLocalizedMessage());
            }

            @Override
            public void onNext(Status args) {
                logger.info("In monitor: args=" + args);
                V2JobMgrIntf jobMgr = jobOps.getJobMgr(args.getJobId());
                if (jobMgr != null) {
                    jobMgr.handleStatus(args);
                    return;
                }
                if (args.getTaskId() != null && !JobFunctions.isV2Task(args.getTaskId())) {
                    Optional<Pair<Job<?>, Task>> jobAndTaskOpt = v3JobOperations.findTaskById(args.getTaskId());
                    if (jobAndTaskOpt.isPresent()) {
                        Task task = jobAndTaskOpt.get().getRight();
                        TaskState newState = V2JobState.toV3TaskState(args.getState());
                        if (task.getStatus().getState() != newState) {

                            String reasonCode = V2JobState.toV3ReasonCode(args.getState(), args.getReason());

                            // We send kill operation even if task is in Accepted state, but if the latter is the case
                            // we do not want to report Mesos 'lost' state in task status.
                            if (isKillConfirmationForTaskInAcceptedState(task, newState, reasonCode)) {
                                reasonCode = TaskStatus.REASON_TASK_KILLED;
                            }
                            TaskStatus taskStatus = JobModel.newTaskStatus()
                                    .withState(newState)
                                    .withReasonCode(reasonCode)
                                    .withReasonMessage("Mesos task state change event: " + args.getMessage())
                                    .withTimestamp(args.getTimestamp())
                                    .build();

                            // Failures are logged only, as the reconciler will take care of it if needed.
                            final Function<Task, Optional<Task>> updater = JobManagerUtil.newMesosTaskStateUpdater(taskStatus, args.getData());
                            v3JobOperations.updateTask(task.getId(), updater, Trigger.Mesos, "Mesos -> " + taskStatus).subscribe(
                                    () -> logger.info("Changed task {} status state to {}", task.getId(), taskStatus),
                                    e -> logger.warn("Could not update task state of {} to {} ({})", args.getTaskId(), taskStatus, e.toString())
                            );
                        }
                        return;
                    }
                }
                killOrphanedTask(args);
            }
        });
        allStatusSubject = PublishSubject.create();
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

    private void killOrphanedTask(Status status) {
        String taskId = status.getTaskId();

        // This should never happen, but lets check it anyway
        if (taskId == null) {
            logger.warn("Task status update notification received, but no task id is given: {}", status);
            return;
        }

        // If it is already terminated, do nothing
        if (V2JobState.isTerminalState(status.getState())) {
            return;
        }

        logger.warn("Received Mesos callback for unknown task: {} (state {}). Terminating it.", taskId, status.getState());
        vmService.killTask(taskId);
    }

    @PreDestroy
    public void shutdown() {
        shutdownFlag.set(true);
    }

    @Override
    public Observable<Status> getAllStatusObservable() {
        return allStatusSubject;
    }

    private void handleJobStuck(StateToMonitor stateToMonitor) {
        if (!shutdownFlag.get()) {
            V2JobMgrIntf jobMgr = stateToMonitor.jobMgr;
            if (jobMgr == null) {
                logger.warn("Can't find jobMgr to handle stuck worker for " + stateToMonitor +
                        ", has null jobMgr, ignoring...");
            } else {
                jobMgr.handleTaskStuckInState(stateToMonitor.taskId, stateToMonitor.state);
            }
        }
    }

    private long getMillisToWait(V2JobState state, V2JobMgrIntf jobMgr) {
        switch (state) {
            case Accepted:
            case Started:
                // TODO Accepted and Started are stable states. Do we need timeout here?
                return 6000;
            case Launched:
                return jobManagerConfiguration.getTaskInLaunchedStateTimeoutMs();
            case StartInitiated:
                return isServiceJob(jobMgr)
                        ? jobManagerConfiguration.getServiceTaskInStartInitiatedStateTimeoutMs()
                        : jobManagerConfiguration.getBatchTaskInStartInitiatedStateTimeoutMs();
            default:
                return 0; // For now, not interested in monitoring other states in which worker could be stuck
        }
    }

    private boolean isServiceJob(V2JobMgrIntf jobMgr) {
        try {
            V2JobMetadata jobMetadata = jobMgr.getJobMetadata();
            return jobMetadata != null && Parameters.getJobType(jobMetadata.getParameters()) == Parameters.JobType.Service;
        } catch (Exception e) {
            logger.warn("Unexpected error during handling task stuck state", e);
            return false;
        }
    }

    private void subscribeToJobMgr(final V2JobMgrIntf jobMgr) {
        jobMgr.getStatusSubject().subscribe(status -> {
            if (status.getWorkerIndex() < 0) {
                return; // its on the job itself, nothing to do
            }
            workerStatesSubject.onNext(new StateToMonitor(jobMgr, status.getWorkerIndex(), status.getWorkerNumber(),
                    WorkerNaming.getWorkerName(jobMgr.getJobId(), status.getWorkerIndex(), status.getWorkerNumber()),
                    status.getState()));
            allStatusSubject.onNext(status);
        });
    }

    public void start(List<V2JobMgrIntf> initialJobMgrs) {
        if (initialJobMgrs != null) {
            for (V2JobMgrIntf jobMgr : initialJobMgrs) {
                subscribeToJobMgr(jobMgr);
            }
        }
        jobCreationObservable.subscribe(jobMgr -> {
            if (jobMgr.getStatusSubject() == null) {
                return;
            }
            subscribeToJobMgr(jobMgr);
        });
    }
}
