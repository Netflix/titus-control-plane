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

package io.netflix.titus.master.jobmanager.service.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.limiter.ImmutableLimiters;
import io.netflix.titus.common.util.retry.Retryers;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.common.interceptor.RateLimiterInterceptor;
import io.netflix.titus.master.jobmanager.service.common.interceptor.RetryActionInterceptor;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static io.netflix.titus.master.jobmanager.service.batch.action.CreateOrReplaceBatchTaskAction.createOrReplaceTaskAction;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.areEquivalent;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.findTaskStateTimeouts;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.hasJobState;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.removeCompletedJob;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.shouldRetry;

@Singleton
public class BatchDifferenceResolver implements ReconciliationEngine.DifferenceResolver<JobChange, JobManagerReconcilerEvent> {

    private static final long NEW_TASK_BUCKET = 10;
    private static final long NEW_TASK_REFILL_INTERVAL_MS = 100;

    private final JobManagerConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService schedulingService;
    private final VirtualMachineMasterService vmService;
    private final JobStore titusStore;

    private final RetryActionInterceptor storeWriteRetryInterceptor;
    private final RateLimiterInterceptor newTaskRateLimiterInterceptor;

    private final Clock clock = Clocks.system();

    @Inject
    public BatchDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore titusStore) {
        this(configuration, capacityGroupService, schedulingService, vmService, titusStore, Schedulers.computation());
    }

    public BatchDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore titusStore,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.vmService = vmService;
        this.titusStore = titusStore;

        this.storeWriteRetryInterceptor = new RetryActionInterceptor(
                "storeWrite",
                Retryers.exponentialBackoff(5000, 5000, TimeUnit.MILLISECONDS),
                scheduler
        );

        this.newTaskRateLimiterInterceptor = new RateLimiterInterceptor(
                "newTask",
                ImmutableLimiters.tokenBucket(
                        NEW_TASK_BUCKET,
                        ImmutableLimiters.refillAtFixedInterval(1, NEW_TASK_REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS)
                )
        );
    }

    @Override
    public List<ChangeAction<JobChange>> apply(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine) {
        List<ChangeAction<JobChange>> actions = new ArrayList<>();
        BatchJobView refJobView = new BatchJobView(engine.getReferenceView());
        EntityHolder storeModel = engine.getStoreView();
        actions.addAll(applyRuntime(engine, refJobView, engine.getRunningView(), storeModel));
        actions.addAll(applyStore(engine, refJobView, storeModel));

        if (actions.isEmpty()) {
            actions.addAll(removeCompletedJob(engine.getReferenceView(), storeModel, titusStore));
        }

        return actions;
    }

    private List<ChangeAction<JobChange>> applyRuntime(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine, BatchJobView refJobView, EntityHolder runningModel, EntityHolder storeModel) {
        List<ChangeAction<JobChange>> actions = new ArrayList<>();
        EntityHolder referenceModel = refJobView.getJobHolder();
        BatchJobView runningJobView = new BatchJobView(runningModel);

        if (hasJobState(referenceModel, JobState.KillInitiated)) {
            return KillInitiatedActions.applyKillInitiated(engine, vmService, TaskStatus.REASON_TASK_KILLED, "Killing task as its job is in KillInitiated state");
        } else if (hasJobState(referenceModel, JobState.Finished)) {
            return Collections.emptyList();
        }

        actions.addAll(findJobSizeInconsistencies(refJobView, storeModel));
        actions.addAll(findMissingRunningTasks(refJobView, runningJobView));
        actions.addAll(findTaskStateTimeouts(engine, runningJobView, configuration, clock, vmService));

        return actions;
    }

    /**
     * Check that the reference job has the required number of tasks.
     */
    private List<ChangeAction<JobChange>> findJobSizeInconsistencies(BatchJobView refJobView, EntityHolder storeModel) {
        boolean canUpdateStore = storeWriteRetryInterceptor.executionLimits(storeModel);
        if (canUpdateStore && refJobView.getTasks().size() < refJobView.getRequiredSize()) {
            List<ChangeAction<JobChange>> missingTasks = new ArrayList<>();
            for (int i = 0; i < refJobView.getRequiredSize(); i++) {
                if (!refJobView.getIndexes().contains(i)) {
                    missingTasks.add(createNewTaskAction(refJobView, i));
                }
            }
            return missingTasks;
        }
        return Collections.emptyList();
    }

    private TitusChangeAction createNewTaskAction(BatchJobView refJobView, int taskIndex) {
        return newTaskRateLimiterInterceptor.apply(
                storeWriteRetryInterceptor.apply(
                        createOrReplaceTaskAction(titusStore, refJobView.getJob(), refJobView.getTasks(), taskIndex)
                )
        );
    }

    /**
     * Check that for each reference job task, there is a corresponding running task.
     */
    private List<ChangeAction<JobChange>> findMissingRunningTasks(BatchJobView refJobView, BatchJobView runningJobView) {
        List<ChangeAction<JobChange>> missingTasks = new ArrayList<>();
        long allowedToRun = newTaskRateLimiterInterceptor.executionLimits(runningJobView.getJobHolder());
        List<BatchJobTask> tasks = refJobView.getTasks();
        for (int i = 0; i < tasks.size() && allowedToRun > 0; i++) {
            BatchJobTask refTask = tasks.get(i);
            BatchJobTask runningTask = runningJobView.getTaskById(refTask.getId());
            if (runningTask == null) {
                missingTasks.add(BasicTaskActions.scheduleTask(capacityGroupService, schedulingService, runningJobView.getJob(), refTask));
                allowedToRun--;
            }
        }
        return missingTasks;
    }

    private List<ChangeAction<JobChange>> applyStore(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine, BatchJobView refJobView, EntityHolder storeJob) {
        if (!storeWriteRetryInterceptor.executionLimits(storeJob)) {
            return Collections.emptyList();
        }

        List<ChangeAction<JobChange>> actions = new ArrayList<>();

        EntityHolder refJobHolder = refJobView.getJobHolder();
        Job<BatchJobExt> refJob = refJobHolder.getEntity();

        if (!refJobHolder.getEntity().equals(storeJob.getEntity())) {
            actions.add(storeWriteRetryInterceptor.apply(BasicJobActions.updateJobInStore(engine, titusStore)));
        }
        boolean isJobTerminating = refJob.getStatus().getState() == JobState.KillInitiated;
        for (EntityHolder referenceTask : refJobHolder.getChildren()) {
            Optional<EntityHolder> storeHolder = storeJob.findById(referenceTask.getId());
            boolean refAndStoreInSync = storeHolder.isPresent() && areEquivalent(storeHolder.get(), referenceTask);
            if (refAndStoreInSync) {
                BatchJobTask task = storeHolder.get().getEntity();
                if (!isJobTerminating && shouldRetry(refJob, task)) {
                    actions.add(createNewTaskAction(refJobView, task.getIndex()));
                }
            } else {
                Task task = referenceTask.getEntity();
                actions.add(storeWriteRetryInterceptor.apply(BasicTaskActions.writeReferenceTaskToStore(titusStore, schedulingService, capacityGroupService, engine, task.getId())));
            }
        }
        return actions;
    }

    static class BatchJobView extends DifferenceResolverUtils.JobView<BatchJobExt, BatchJobTask> {

        private final Set<Integer> indexes;

        BatchJobView(EntityHolder jobHolder) {
            super(jobHolder);
            this.indexes = getTasks().stream().map(BatchJobTask::getIndex).collect(Collectors.toSet());
        }

        Set<Integer> getIndexes() {
            return indexes;
        }
    }
}
