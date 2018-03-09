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
import java.util.concurrent.atomic.AtomicInteger;
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
import io.netflix.titus.common.util.retry.Retryers;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import io.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.common.interceptor.RetryActionInterceptor;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static io.netflix.titus.master.jobmanager.service.batch.action.CreateOrReplaceBatchTaskActions.createOrReplaceTaskAction;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.areEquivalent;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.findTaskStateTimeouts;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.hasJobState;
import static io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.shouldRetry;

@Singleton
public class BatchDifferenceResolver implements ReconciliationEngine.DifferenceResolver<JobManagerReconcilerEvent> {

    private static final Logger logger = LoggerFactory.getLogger(BatchDifferenceResolver.class);

    private final JobManagerConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService schedulingService;
    private final VirtualMachineMasterService vmService;
    private final JobStore jobStore;

    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;

    private final RetryActionInterceptor storeWriteRetryInterceptor;

    private final Clock clock;

    @Inject
    public BatchDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint) {
        this(configuration, capacityGroupService, schedulingService, vmService, jobStore, constraintEvaluatorTransformer,
                systemSoftConstraint, systemHardConstraint, Clocks.system(), Schedulers.computation());
    }

    public BatchDifferenceResolver(
            JobManagerConfiguration configuration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint,
            Clock clock,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.vmService = vmService;
        this.jobStore = jobStore;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        this.clock = clock;

        this.storeWriteRetryInterceptor = new RetryActionInterceptor(
                "storeWrite",
                Retryers.exponentialBackoff(500, 5000, TimeUnit.MILLISECONDS),
                scheduler
        );
    }

    @Override
    public List<ChangeAction> apply(ReconciliationEngine<JobManagerReconcilerEvent> engine) {
        List<ChangeAction> actions = new ArrayList<>();
        BatchJobView refJobView = new BatchJobView(engine.getReferenceView());
        EntityHolder storeModel = engine.getStoreView();

        int activeNotStartedTasks = DifferenceResolverUtils.countActiveNotStartedTasks(refJobView.getJobHolder(), engine.getRunningView());
        AtomicInteger allowedNewTasks = new AtomicInteger(Math.max(0, configuration.getActiveNotStartedTasksLimit() - activeNotStartedTasks));

        actions.addAll(applyStore(engine, refJobView, storeModel, allowedNewTasks));
        actions.addAll(applyRuntime(engine, refJobView, engine.getRunningView(), storeModel, allowedNewTasks));

        if (actions.isEmpty()) {
            actions.addAll(removeCompletedJob(engine.getReferenceView(), storeModel, jobStore));
        }

        return actions;
    }

    private List<ChangeAction> applyRuntime(ReconciliationEngine<JobManagerReconcilerEvent> engine, BatchJobView refJobView, EntityHolder runningModel, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        List<ChangeAction> actions = new ArrayList<>();
        EntityHolder referenceModel = refJobView.getJobHolder();
        BatchJobView runningJobView = new BatchJobView(runningModel);

        if (hasJobState(referenceModel, JobState.KillInitiated)) {
            List<ChangeAction> killInitiatedActions = KillInitiatedActions.reconcilerInitiatedAllTasksKillInitiated(engine, vmService, jobStore, TaskStatus.REASON_TASK_KILLED, "Killing task as its job is in KillInitiated state");
            if (killInitiatedActions.isEmpty()) {
                return findTaskStateTimeouts(engine, runningJobView, configuration, clock, vmService, jobStore);
            }
            return killInitiatedActions;
        } else if (hasJobState(referenceModel, JobState.Finished)) {
            return Collections.emptyList();
        }

        List<ChangeAction> numberOfTaskAdjustingActions = findJobSizeInconsistencies(refJobView, storeModel, allowedNewTasks);
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
    private List<ChangeAction> findJobSizeInconsistencies(BatchJobView refJobView, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        boolean canUpdateStore = storeWriteRetryInterceptor.executionLimits(storeModel);
        if (canUpdateStore && refJobView.getTasks().size() < refJobView.getRequiredSize()) {
            List<ChangeAction> missingTasks = new ArrayList<>();
            for (int i = 0; i < refJobView.getRequiredSize() && allowedNewTasks.get() > 0; i++) {
                if (!refJobView.getIndexes().contains(i)) {
                    allowedNewTasks.decrementAndGet();
                    logger.info("Adding missing task: jobId={}, index={}, requiredSize={}, currentSize={}", refJobView.getJob().getId(), i, refJobView.getRequiredSize(), refJobView.getTasks().size());
                    missingTasks.add(createNewTaskAction(refJobView, i));
                }
            }
            return missingTasks;
        }
        return Collections.emptyList();
    }

    private TitusChangeAction createNewTaskAction(BatchJobView refJobView, int taskIndex) {
        return storeWriteRetryInterceptor.apply(
                createOrReplaceTaskAction(configuration, jobStore, refJobView.getJobHolder(), taskIndex, clock)
        );
    }

    /**
     * Check that for each reference job task, there is a corresponding running task.
     */
    private List<ChangeAction> findMissingRunningTasks(ReconciliationEngine<JobManagerReconcilerEvent> engine, BatchJobView refJobView, BatchJobView runningJobView) {
        List<ChangeAction> missingTasks = new ArrayList<>();
        List<BatchJobTask> tasks = refJobView.getTasks();
        for (BatchJobTask refTask : tasks) {
            BatchJobTask runningTask = runningJobView.getTaskById(refTask.getId());
            if (runningTask == null) {
                missingTasks.add(BasicTaskActions.scheduleTask(capacityGroupService,
                        schedulingService,
                        runningJobView.getJob(),
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

    private List<ChangeAction> applyStore(ReconciliationEngine<JobManagerReconcilerEvent> engine, BatchJobView refJobView, EntityHolder storeJob, AtomicInteger allowedNewTasks) {
        if (!storeWriteRetryInterceptor.executionLimits(storeJob)) {
            return Collections.emptyList();
        }

        List<ChangeAction> actions = new ArrayList<>();

        EntityHolder refJobHolder = refJobView.getJobHolder();
        Job<BatchJobExt> refJob = refJobHolder.getEntity();

        if (!refJobHolder.getEntity().equals(storeJob.getEntity())) {
            actions.add(storeWriteRetryInterceptor.apply(BasicJobActions.updateJobInStore(engine, jobStore)));
        }
        boolean isJobTerminating = refJob.getStatus().getState() == JobState.KillInitiated;
        for (EntityHolder referenceTask : refJobHolder.getChildren()) {

            Optional<EntityHolder> storeHolder = storeJob.findById(referenceTask.getId());
            BatchJobTask storeTask = storeHolder.get().getEntity();

            boolean refAndStoreInSync = storeHolder.isPresent() && areEquivalent(storeHolder.get(), referenceTask);
            boolean shouldRetry = !isJobTerminating && shouldRetry(refJob, referenceTask.getEntity()) && allowedNewTasks.get() > 0;

            if (refAndStoreInSync) {
                if (shouldRetry && TaskRetryers.shouldRetryNow(referenceTask, clock)) {
                    logger.info("Retrying task: oldTaskId={}, index={}", referenceTask.getId(), storeTask.getIndex());
                    actions.add(createNewTaskAction(refJobView, storeTask.getIndex()));
                }
            } else {
                Task task = referenceTask.getEntity();
                actions.add(storeWriteRetryInterceptor.apply(BasicTaskActions.writeReferenceTaskToStore(jobStore, schedulingService, capacityGroupService, engine, task.getId())));
            }
            // Both current and delayed retries are counted
            if (shouldRetry) {
                allowedNewTasks.decrementAndGet();
            }
        }
        return actions;
    }

    private static List<ChangeAction> removeCompletedJob(EntityHolder referenceModel, EntityHolder storeModel, JobStore titusStore) {
        if (!hasJobState(referenceModel, JobState.Finished)) {
            if (DifferenceResolverUtils.allDone(storeModel)) {
                return Collections.singletonList(BasicJobActions.completeJob(referenceModel.getId()));
            }
        } else {
            if (!BasicJobActions.isClosed(referenceModel)) {
                return Collections.singletonList(BasicJobActions.removeJobFromStore(referenceModel.getEntity(), titusStore));
            }

        }
        return Collections.emptyList();
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
