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

package io.netflix.titus.master.jobmanager.service.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.retry.Retryers;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import io.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.common.interceptor.RetryActionInterceptor;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.areEquivalent;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.findTaskStateTimeouts;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.hasJobState;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.isTerminating;
import static io.netflix.titus.master.jobmanager.service.service.action.BasicServiceTaskActions.removeFinishedServiceTaskAction;
import static io.netflix.titus.master.jobmanager.service.service.action.CreateOrReplaceServiceTaskActions.createOrReplaceTaskAction;


@Singleton
public class ServiceDifferenceResolver implements ReconciliationEngine.DifferenceResolver<JobManagerReconcilerEvent> {

    private final JobManagerConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService schedulingService;
    private final VirtualMachineMasterService vmService;
    private final JobStore jobStore;

    private final RetryActionInterceptor storeWriteRetryInterceptor;

    private final Clock clock;

    @Inject
    public ServiceDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore) {
        this(configuration, capacityGroupService, schedulingService, vmService, jobStore, Clocks.system(), Schedulers.computation());
    }

    public ServiceDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            Clock clock,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.vmService = vmService;
        this.jobStore = jobStore;
        this.clock = clock;

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
        List<ChangeAction> actions = new ArrayList<>();
        EntityHolder referenceModel = refJobView.getJobHolder();
        ServiceJobView runningJobView = new ServiceJobView(runningModel);

        if (hasJobState(referenceModel, JobState.KillInitiated)) {
            return KillInitiatedActions.reconcilerInitiatedAllTasksKillInitiated(engine, vmService, jobStore, TaskStatus.REASON_TASK_KILLED, "Killing task as its job is in KillInitiated state");
        } else if (hasJobState(referenceModel, JobState.Finished)) {
            return Collections.emptyList();
        }

        actions.addAll(findJobSizeInconsistencies(engine, refJobView, storeModel, allowedNewTasks));
        actions.addAll(findMissingRunningTasks(refJobView, runningJobView));
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
                missingTasks.add(createNewTaskAction(refJobView, Optional.empty()));
            }
            return missingTasks;
        } else if (missing < 0) {
            // Too many tasks (job was scaled down)
            int finishedCount = (int) tasks.stream().filter(DifferenceResolverUtils::isTerminating).count();
            int toRemoveCount = -missing - finishedCount;
            if (toRemoveCount > 0) {
                List<ChangeAction> toRemove = new ArrayList<>();
                for (int i = tasks.size() - 1; toRemoveCount > 0 && i >= 0; i--) {
                    ServiceJobTask next = tasks.get(i);
                    if (!isTerminating(next)) {
                        toRemove.add(KillInitiatedActions.reconcilerInitiatedTaskKillInitiated(engine, next, vmService, jobStore, TaskStatus.REASON_SCALED_DOWN, "Terminating excessive service job task"));
                        toRemoveCount--;
                    }
                }
                return toRemove;
            }
        }
        return Collections.emptyList();
    }

    private TitusChangeAction createNewTaskAction(ServiceJobView refJobView, Optional<EntityHolder> previousTask) {
        return storeWriteRetryInterceptor.apply(createOrReplaceTaskAction(configuration, jobStore, refJobView.getJobHolder(), previousTask, clock));
    }

    /**
     * Check that for each reference job task, there is a corresponding running task.
     */
    private List<ChangeAction> findMissingRunningTasks(ServiceJobView refJobView, ServiceJobView runningJobView) {
        List<ChangeAction> missingTasks = new ArrayList<>();
        List<ServiceJobTask> tasks = refJobView.getTasks();
        for (int i = 0; i < tasks.size(); i++) {
            ServiceJobTask refTask = tasks.get(i);
            ServiceJobTask runningTask = runningJobView.getTaskById(refTask.getId());
            if (runningTask == null) {
                missingTasks.add(BasicTaskActions.scheduleTask(capacityGroupService, schedulingService, runningJobView.getJob(), refTask));
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
                if(currentTaskState == TaskState.Finished) {
                    if (isJobTerminating || TaskStatus.REASON_SCALED_DOWN.equals(storeTask.getStatus().getReasonCode())) {
                        actions.add(removeFinishedServiceTaskAction(storeTask));
                    } else if (shouldRetry && TaskRetryers.shouldRetryNow(referenceTaskHolder, clock)) {
                        actions.add(createNewTaskAction(refJobView, Optional.of(referenceTaskHolder)));
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
