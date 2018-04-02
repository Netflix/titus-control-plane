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

package com.netflix.titus.master.jobmanager.service.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.VirtualMachineMasterService;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import com.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import com.netflix.titus.master.jobmanager.service.common.interceptor.RetryActionInterceptor;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.areEquivalent;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.findTaskStateTimeouts;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.hasJobState;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.isTerminating;
import static com.netflix.titus.master.jobmanager.service.service.action.BasicServiceTaskActions.removeFinishedServiceTaskAction;
import static com.netflix.titus.master.jobmanager.service.service.action.CreateOrReplaceServiceTaskActions.createOrReplaceTaskAction;


@Singleton
public class ServiceDifferenceResolver implements ReconciliationEngine.DifferenceResolver<JobManagerReconcilerEvent> {

    private final JobManagerConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService schedulingService;
    private final VirtualMachineMasterService vmService;
    private final JobStore jobStore;
    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;

    private final RetryActionInterceptor storeWriteRetryInterceptor;

    private final CodeInvariants codeInvariants;
    private final Clock clock;

    @Inject
    public ServiceDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint,
            TitusRuntime titusRuntime) {
        this(configuration, capacityGroupService, schedulingService, vmService, jobStore, constraintEvaluatorTransformer,
                systemSoftConstraint, systemHardConstraint, titusRuntime, Schedulers.computation());
    }

    public ServiceDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint,
            TitusRuntime titusRuntime,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.vmService = vmService;
        this.jobStore = jobStore;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        this.clock = titusRuntime.getClock();
        this.codeInvariants = titusRuntime.getCodeInvariants();

        this.storeWriteRetryInterceptor = new RetryActionInterceptor(
                "storeWrite",
                Retryers.exponentialBackoff(5000, 5000, TimeUnit.MILLISECONDS),
                scheduler
        );
    }

    @Override
    public List<ChangeAction> apply(ReconciliationEngine<JobManagerReconcilerEvent> engine) {
        List<ChangeAction> actions = new ArrayList<>();
        ServiceJobView refJobView = new ServiceJobView(engine.getReferenceView());

        int activeNotStartedTasks = DifferenceResolverUtils.countActiveNotStartedTasks(refJobView.getJobHolder(), engine.getRunningView());
        AtomicInteger allowedNewTasks = new AtomicInteger(Math.max(0, configuration.getActiveNotStartedTasksLimit() - activeNotStartedTasks));

        actions.addAll(applyStore(engine, refJobView, engine.getStoreView(), allowedNewTasks));
        actions.addAll(applyRuntime(engine, refJobView, engine.getRunningView(), engine.getStoreView(), allowedNewTasks));

        if (actions.isEmpty()) {
            actions.addAll(removeCompletedJob(engine.getReferenceView(), engine.getStoreView(), jobStore));
        }

        return actions;
    }

    private List<ChangeAction> applyRuntime(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobView refJobView, EntityHolder runningModel, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        EntityHolder referenceModel = refJobView.getJobHolder();
        ServiceJobView runningJobView = new ServiceJobView(runningModel);

        if (hasJobState(referenceModel, JobState.KillInitiated)) {
            List<ChangeAction> killInitiatedActions = KillInitiatedActions.reconcilerInitiatedAllTasksKillInitiated(
                    engine, vmService, jobStore, TaskStatus.REASON_TASK_KILLED, "Killing task as its job is in KillInitiated state"
            );
            if (killInitiatedActions.isEmpty()) {
                return findTaskStateTimeouts(engine, runningJobView, configuration, clock, vmService, jobStore);
            }
            return killInitiatedActions;
        } else if (hasJobState(referenceModel, JobState.Finished)) {
            return Collections.emptyList();
        }

        List<ChangeAction> actions = new ArrayList<>();
        List<ChangeAction> numberOfTaskAdjustingActions = findJobSizeInconsistencies(engine, refJobView, storeModel, allowedNewTasks);
        actions.addAll(numberOfTaskAdjustingActions);
        if (numberOfTaskAdjustingActions.isEmpty()) {
            actions.addAll(findMissingRunningTasks(engine, refJobView, runningJobView));
        }
        actions.addAll(findTaskStateTimeouts(engine, runningJobView, configuration, clock, vmService, jobStore));

        return actions;
    }

    /**
     * Check that the reference job has the required number of tasks.
     */
    private List<ChangeAction> findJobSizeInconsistencies(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobView refJobView, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        boolean canUpdateStore = storeWriteRetryInterceptor.executionLimits(storeModel);
        List<ServiceJobTask> tasks = refJobView.getTasks();
        int missing = refJobView.getRequiredSize() - tasks.size();
        if (canUpdateStore && missing > 0) {
            List<ChangeAction> missingTasks = new ArrayList<>();
            for (int i = 0; i < missing && allowedNewTasks.get() > 0; i++) {
                allowedNewTasks.decrementAndGet();
                createNewTaskAction(refJobView, Optional.empty()).ifPresent(missingTasks::add);
            }
            return missingTasks;
        } else if (missing < 0) {
            // Too many tasks (job was scaled down)
            int finishedCount = (int) tasks.stream().filter(t -> t.getStatus().getState() == TaskState.Finished).count();
            int toRemoveCount = -missing - finishedCount;
            if (toRemoveCount > 0) {
                List<ServiceJobTask> tasksToRemove = ScaleDownEvaluator.selectTasksToTerminate(tasks, tasks.size() - toRemoveCount);
                return tasksToRemove.stream()
                        .filter(t -> !isTerminating(t))
                        .map(t -> KillInitiatedActions.reconcilerInitiatedTaskKillInitiated(engine, t, vmService, jobStore, TaskStatus.REASON_SCALED_DOWN, "Terminating excessive service job task"))
                        .collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }

    private Optional<TitusChangeAction> createNewTaskAction(ServiceJobView refJobView, Optional<EntityHolder> previousTask) {
        // Safety check
        long numberOfNotFinishedTasks = refJobView.getJobHolder().getChildren().stream()
                .filter(holder -> TaskState.isRunning(((Task) holder.getEntity()).getStatus().getState()))
                .count();
        if (numberOfNotFinishedTasks >= refJobView.getRequiredSize()) {
            codeInvariants.inconsistent(
                    "Service job reconciler attempts to create too many tasks: jobId=%s, requiredSize=%s, current=%s",
                    refJobView.getJob().getId(), refJobView.getRequiredSize(), numberOfNotFinishedTasks
            );
            return Optional.empty();
        }


        TitusChangeAction storeAction = storeWriteRetryInterceptor.apply(
                createOrReplaceTaskAction(configuration, jobStore, refJobView.getJobHolder(), previousTask, clock)
        );
        return Optional.of(storeAction);
    }

    /**
     * Check that for each reference job task, there is a corresponding running task.
     */
    private List<ChangeAction> findMissingRunningTasks(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobView refJobView, ServiceJobView runningJobView) {
        List<ChangeAction> missingTasks = new ArrayList<>();
        List<ServiceJobTask> tasks = refJobView.getTasks();
        for (ServiceJobTask refTask : tasks) {
            ServiceJobTask runningTask = runningJobView.getTaskById(refTask.getId());
            if (runningTask == null) {
                missingTasks.add(BasicTaskActions.scheduleTask(
                        capacityGroupService,
                        schedulingService,
                        refJobView.getJob(),
                        refTask,
                        () -> JobManagerUtil.filterActiveTaskIds(engine),
                        constraintEvaluatorTransformer,
                        systemSoftConstraint,
                        systemHardConstraint
                ));
            }
        }
        return missingTasks;
    }

    private List<ChangeAction> applyStore(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobView refJobView, EntityHolder storeJob, AtomicInteger allowedNewTasks) {
        if (!storeWriteRetryInterceptor.executionLimits(storeJob)) {
            return Collections.emptyList();
        }

        List<ChangeAction> actions = new ArrayList<>();

        EntityHolder refJobHolder = refJobView.getJobHolder();
        Job<ServiceJobExt> refJob = refJobHolder.getEntity();

        if (!refJobHolder.getEntity().equals(storeJob.getEntity())) {
            actions.add(storeWriteRetryInterceptor.apply(BasicJobActions.updateJobInStore(engine, jobStore)));
        }
        boolean isJobTerminating = refJob.getStatus().getState() == JobState.KillInitiated;
        for (EntityHolder referenceTaskHolder : refJobHolder.getChildren()) {
            ServiceJobTask refTask = referenceTaskHolder.getEntity();
            Optional<EntityHolder> storeHolder = storeJob.findById(referenceTaskHolder.getId());
            ServiceJobTask storeTask = storeHolder.get().getEntity();

            boolean refAndStoreInSync = areEquivalent(storeHolder.get(), referenceTaskHolder);
            boolean shouldRetry = !isJobTerminating
                    && refTask.getStatus().getState() == TaskState.Finished
                    && !refTask.getStatus().getReasonCode().equals(TaskStatus.REASON_SCALED_DOWN)
                    && allowedNewTasks.get() > 0;

            if (refAndStoreInSync) {
                TaskState currentTaskState = refTask.getStatus().getState();
                if (currentTaskState == TaskState.Finished) {
                    if (isJobTerminating || isScaledDown(storeTask)) {
                        actions.add(removeFinishedServiceTaskAction(jobStore, storeTask));
                    } else if (shouldRetry && TaskRetryers.shouldRetryNow(referenceTaskHolder, clock)) {
                        createNewTaskAction(refJobView, Optional.of(referenceTaskHolder)).ifPresent(actions::add);
                    }
                }
            } else {
                Task task = referenceTaskHolder.getEntity();
                actions.add(storeWriteRetryInterceptor.apply(BasicTaskActions.writeReferenceTaskToStore(jobStore, schedulingService, capacityGroupService, engine, task.getId())));
            }

            // Both current and delayed retries are counted
            if (shouldRetry) {
                allowedNewTasks.decrementAndGet();
            }
        }
        return actions;
    }

    private boolean isScaledDown(ServiceJobTask task) {
        return JobFunctions.findTaskStatus(task, TaskState.KillInitiated)
                .map(status -> TaskStatus.REASON_SCALED_DOWN.equals(status.getReasonCode()))
                .orElse(false);
    }

    private List<ChangeAction> removeCompletedJob(EntityHolder referenceModel, EntityHolder storeModel, JobStore titusStore) {
        if (!hasJobState(referenceModel, JobState.Finished)) {
            if (hasJobState(referenceModel, JobState.KillInitiated) && DifferenceResolverUtils.allDone(storeModel)) {
                return Collections.singletonList(BasicJobActions.completeJob(referenceModel.getId()));
            }
        } else {
            if (!BasicJobActions.isClosed(referenceModel)) {
                return Collections.singletonList(BasicJobActions.removeJobFromStore(referenceModel.getEntity(), titusStore));
            }

        }
        return Collections.emptyList();
    }

    private static class ServiceJobView extends DifferenceResolverUtils.JobView<ServiceJobExt, ServiceJobTask> {
        ServiceJobView(EntityHolder jobHolder) {
            super(jobHolder);
        }
    }
}
