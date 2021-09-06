/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.jobmanager.service.VersionSupplier;
import com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import com.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import com.netflix.titus.master.jobmanager.service.common.interceptor.RetryActionInterceptor;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.DirectKubeConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.netflix.titus.api.jobmanager.service.JobManagerConstants.RECONCILER_CALLMETADATA;
import static com.netflix.titus.master.jobmanager.service.batch.action.CreateOrReplaceBatchTaskActions.createOrReplaceTaskAction;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.getTaskContext;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.getUnassignedEbsVolumes;
import static com.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils.getUnassignedIpAllocations;

@Singleton
public class BatchDifferenceResolver implements ReconciliationEngine.DifferenceResolver<JobManagerReconcilerEvent> {

    private static final Logger logger = LoggerFactory.getLogger(BatchDifferenceResolver.class);

    private final DirectKubeApiServerIntegrator kubeApiServerIntegrator;
    private final JobManagerConfiguration configuration;
    private final FeatureActivationConfiguration featureConfiguration;
    private final DirectKubeConfiguration kubeConfiguration;
    private final KubePodConfiguration kubePodConfiguration;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService<? extends TaskRequest> schedulingService;
    private final VirtualMachineMasterService vmService;
    private final JobStore jobStore;
    private final VersionSupplier versionSupplier;

    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;

    private final RetryActionInterceptor storeWriteRetryInterceptor;

    private final TokenBucket stuckInStateRateLimiter;
    private final TitusRuntime titusRuntime;
    private final Clock clock;

    @Inject
    public BatchDifferenceResolver(
            DirectKubeApiServerIntegrator kubeApiServerIntegrator,
            JobManagerConfiguration configuration,
            FeatureActivationConfiguration featureConfiguration,
            DirectKubeConfiguration kubeConfiguration,
            KubePodConfiguration kubePodConfiguration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService<? extends TaskRequest> schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            VersionSupplier versionSupplier,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint,
            @Named(JobManagerConfiguration.STUCK_IN_STATE_TOKEN_BUCKET) TokenBucket stuckInStateRateLimiter,
            TitusRuntime titusRuntime) {
        this(kubeApiServerIntegrator, configuration, featureConfiguration, kubeConfiguration, kubePodConfiguration,
                capacityGroupService, schedulingService, vmService, jobStore, versionSupplier,
                constraintEvaluatorTransformer, systemSoftConstraint, systemHardConstraint, stuckInStateRateLimiter,
                titusRuntime, Schedulers.computation()
        );
    }

    public BatchDifferenceResolver(
            DirectKubeApiServerIntegrator kubeApiServerIntegrator,
            JobManagerConfiguration configuration,
            FeatureActivationConfiguration featureConfiguration,
            DirectKubeConfiguration kubeConfiguration,
            KubePodConfiguration kubePodConfiguration,
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService<? extends TaskRequest> schedulingService,
            VirtualMachineMasterService vmService,
            JobStore jobStore,
            VersionSupplier versionSupplier,
            ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
            SystemSoftConstraint systemSoftConstraint,
            SystemHardConstraint systemHardConstraint,
            @Named(JobManagerConfiguration.STUCK_IN_STATE_TOKEN_BUCKET) TokenBucket stuckInStateRateLimiter,
            TitusRuntime titusRuntime,
            Scheduler scheduler) {
        this.kubeApiServerIntegrator = kubeApiServerIntegrator;
        this.configuration = configuration;
        this.featureConfiguration = featureConfiguration;
        this.kubeConfiguration = kubeConfiguration;
        this.kubePodConfiguration = kubePodConfiguration;
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.vmService = vmService;
        this.jobStore = jobStore;
        this.versionSupplier = versionSupplier;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        this.stuckInStateRateLimiter = stuckInStateRateLimiter;
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();

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
            actions.addAll(removeCompletedJob(engine.getReferenceView(), storeModel, jobStore, versionSupplier));
        }

        return actions;
    }

    private List<ChangeAction> applyRuntime(ReconciliationEngine<JobManagerReconcilerEvent> engine, BatchJobView refJobView, EntityHolder runningModel, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        List<ChangeAction> actions = new ArrayList<>();
        EntityHolder referenceModel = refJobView.getJobHolder();
        BatchJobView runningJobView = new BatchJobView(runningModel);

        if (DifferenceResolverUtils.hasJobState(referenceModel, JobState.KillInitiated)) {
            List<ChangeAction> killInitiatedActions = KillInitiatedActions.reconcilerInitiatedAllTasksKillInitiated(
                    engine, vmService, kubeApiServerIntegrator, jobStore, TaskStatus.REASON_TASK_KILLED,
                    "Killing task as its job is in KillInitiated state", configuration.getConcurrentReconcilerStoreUpdateLimit(),
                    versionSupplier,
                    titusRuntime
            );
            if (killInitiatedActions.isEmpty()) {
                return DifferenceResolverUtils.findTaskStateTimeouts(engine, runningJobView, configuration, vmService, kubeApiServerIntegrator, jobStore, versionSupplier, stuckInStateRateLimiter, titusRuntime);
            }
            return killInitiatedActions;
        } else if (DifferenceResolverUtils.hasJobState(referenceModel, JobState.Finished)) {
            return Collections.emptyList();
        }

        List<ChangeAction> numberOfTaskAdjustingActions = findJobSizeInconsistencies(refJobView, storeModel, allowedNewTasks);
        actions.addAll(numberOfTaskAdjustingActions);
        if (numberOfTaskAdjustingActions.isEmpty()) {
            actions.addAll(findMissingRunningTasks(engine, refJobView, runningJobView));
        }
        actions.addAll(DifferenceResolverUtils.findTaskStateTimeouts(engine, runningJobView, configuration, vmService, kubeApiServerIntegrator, jobStore, versionSupplier, stuckInStateRateLimiter, titusRuntime));

        return actions;
    }

    /**
     * Check that the reference job has the required number of tasks.
     */
    private List<ChangeAction> findJobSizeInconsistencies(BatchJobView refJobView, EntityHolder storeModel, AtomicInteger allowedNewTasks) {
        boolean canUpdateStore = storeWriteRetryInterceptor.executionLimits(storeModel);
        List<String> unassignedIpAllocations = getUnassignedIpAllocations(refJobView);
        List<String> unassignedEbsVolumeIds = getUnassignedEbsVolumes(refJobView);
        if (canUpdateStore && refJobView.getTasks().size() < refJobView.getRequiredSize()) {
            List<ChangeAction> missingTasks = new ArrayList<>();
            for (int i = 0; i < refJobView.getRequiredSize() && allowedNewTasks.get() > 0; i++) {
                if (!refJobView.getIndexes().contains(i)) {
                    allowedNewTasks.decrementAndGet();
                    logger.info("Adding missing task: jobId={}, index={}, requiredSize={}, currentSize={}", refJobView.getJob().getId(), i, refJobView.getRequiredSize(), refJobView.getTasks().size());
                    createNewTaskAction(refJobView, i, Optional.empty(), unassignedIpAllocations, unassignedEbsVolumeIds).ifPresent(missingTasks::add);
                }
            }
            return missingTasks;
        }
        return Collections.emptyList();
    }

    private Optional<TitusChangeAction> createNewTaskAction(BatchJobView refJobView, int taskIndex, Optional<EntityHolder> previousTask, List<String> unassignedIpAllocations, List<String> ebsVolumeIds) {
        // Safety check
        long numberOfNotFinishedTasks = refJobView.getJobHolder().getChildren().stream()
                .filter(holder -> TaskState.isRunning(((Task) holder.getEntity()).getStatus().getState()))
                .count();
        if (numberOfNotFinishedTasks >= refJobView.getRequiredSize()) {
            titusRuntime.getCodeInvariants().inconsistent(
                    "Batch job reconciler attempts to create too many tasks: jobId=%s, requiredSize=%s, current=%s",
                    refJobView.getJob().getId(), refJobView.getRequiredSize(), numberOfNotFinishedTasks
            );
            return Optional.empty();
        }

        Map<String, String> taskContext = getTaskContext(previousTask, unassignedIpAllocations, ebsVolumeIds);

        JobDescriptor jobDescriptor = refJobView.getJob().getJobDescriptor();
        ApplicationSLA capacityGroupDescriptor = JobManagerUtil.getCapacityGroupDescriptor(jobDescriptor, capacityGroupService);
        String resourcePool = capacityGroupDescriptor.getResourcePool();
        // TODO: remove this check, it's kept here for integration tests
        // EmbeddedTitusMaster still relies on the Mesos/Fenzo integration
        if (featureConfiguration.isKubeSchedulerEnabled()) {
            taskContext = CollectionsExt.copyAndAdd(taskContext, ImmutableMap.of(
                    TaskAttributes.TASK_ATTRIBUTES_OWNED_BY_KUBE_SCHEDULER, "true",
                    TaskAttributes.TASK_ATTRIBUTES_RESOURCE_POOL, resourcePool,
                    TaskAttributes.TASK_ATTRIBUTES_TIER, capacityGroupDescriptor.getTier().name()));
        }

        TitusChangeAction storeAction = storeWriteRetryInterceptor.apply(
                createOrReplaceTaskAction(configuration, jobStore, refJobView.getJobHolder(), taskIndex, versionSupplier, clock, taskContext)
        );
        return Optional.of(storeAction);
    }

    /**
     * Check that for each reference job task, there is a corresponding running task.
     */
    private List<ChangeAction> findMissingRunningTasks(ReconciliationEngine<JobManagerReconcilerEvent> engine, BatchJobView refJobView, BatchJobView runningJobView) {
        List<ChangeAction> missingTasks = new ArrayList<>();
        List<BatchJobTask> tasks = refJobView.getTasks();
        for (BatchJobTask refTask : tasks) {
            BatchJobTask runningTask = runningJobView.getTaskById(refTask.getId());
            if (JobFunctions.isOwnedByKubeScheduler(refTask)) {
                if (kubeApiServerIntegrator.isReadyForScheduling()) {
                    // TODO This complexity exists due to the way Fenzo is initialized on bootstrap. This code can be simplified one we move off Fenzo.
                    if (runningTask == null || (refTask.getStatus().getState() == TaskState.Accepted && !TaskStatus.hasPod(refTask))) {
                        missingTasks.add(BasicTaskActions.launchTaskInKube(
                                kubeApiServerIntegrator,
                                configuration,
                                engine,
                                runningJobView.getJob(),
                                refTask,
                                RECONCILER_CALLMETADATA.toBuilder().withCallReason("Launching task in Kube").build(),
                                versionSupplier,
                                titusRuntime
                        ));
                    }
                }
            } else {
                if (runningTask == null) {
                    CallMetadata callMetadata = RECONCILER_CALLMETADATA.toBuilder().withCallReason("Add task to Fenzo").build();
                    missingTasks.add(BasicTaskActions.scheduleTask(capacityGroupService,
                            schedulingService,
                            runningJobView.getJob(),
                            refTask,
                            featureConfiguration::isOpportunisticResourcesSchedulingEnabled,
                            () -> JobManagerUtil.filterActiveTaskIds(engine),
                            constraintEvaluatorTransformer,
                            systemSoftConstraint,
                            systemHardConstraint,
                            callMetadata
                    ));
                }
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

            boolean refAndStoreInSync = storeHolder.isPresent() && DifferenceResolverUtils.areEquivalent(storeHolder.get(), referenceTask);
            boolean shouldRetry = !isJobTerminating && DifferenceResolverUtils.shouldRetry(refJob, referenceTask.getEntity()) && allowedNewTasks.get() > 0;

            if (refAndStoreInSync) {
                BatchJobTask storeTask = storeHolder.get().getEntity();
                if (shouldRetry && TaskRetryers.shouldRetryNow(referenceTask, clock)) {
                    logger.info("Retrying task: oldTaskId={}, index={}", referenceTask.getId(), storeTask.getIndex());
                    createNewTaskAction(refJobView, storeTask.getIndex(), Optional.of(referenceTask), Collections.emptyList(), Collections.emptyList()).ifPresent(actions::add);
                }
            } else {
                Task task = referenceTask.getEntity();
                CallMetadata callMetadata = RECONCILER_CALLMETADATA.toBuilder().withCallReason("Writing runtime state changes to store").build();
                actions.add(storeWriteRetryInterceptor.apply(BasicTaskActions.writeReferenceTaskToStore(jobStore, schedulingService, engine, task.getId(), callMetadata, titusRuntime)));
            }
            // Both current and delayed retries are counted
            if (shouldRetry) {
                allowedNewTasks.decrementAndGet();
            }
        }
        return actions;
    }

    private static List<ChangeAction> removeCompletedJob(EntityHolder referenceModel, EntityHolder storeModel,
                                                         JobStore titusStore, VersionSupplier versionSupplier) {
        if (!DifferenceResolverUtils.hasJobState(referenceModel, JobState.Finished)) {
            if (DifferenceResolverUtils.allDone(storeModel)) {
                return Collections.singletonList(BasicJobActions.completeJob(referenceModel.getId(), versionSupplier));
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
            this.indexes = collectIndexes();
        }

        Set<Integer> getIndexes() {
            return indexes;
        }

        private Set<Integer> collectIndexes() {
            Set<Integer> indexes = new HashSet<>();
            for (BatchJobTask task : getTasks()) {
                indexes.add(task.getIndex());
            }
            return indexes;
        }
    }
}
