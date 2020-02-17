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

package com.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.DifferenceResolvers;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import com.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import com.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationFramework;
import com.netflix.titus.common.framework.reconciler.internal.InternalReconciliationEngine;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.EntitySanitizerUtil;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.jobmanager.service.DefaultV3JobOperations.IndexKind;
import com.netflix.titus.master.jobmanager.service.common.SecurityGroupUtils;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions;
import com.netflix.titus.master.jobmanager.service.event.JobEventFactory;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_PERMISSIVE_SANITIZER;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

/**
 * Helper class that encapsulates the creation process of job {@link ReconciliationFramework}.
 */
@Singleton
public class JobReconciliationFrameworkFactory {

    private static final Logger logger = LoggerFactory.getLogger(JobReconciliationFrameworkFactory.class);

    public static final String INCONSISTENT_DATA_FAILURE_ID = V3JobOperations.COMPONENT + ".inconsistentData";

    static final String ROOT_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.bootstrap.";
    private final FeatureActivationConfiguration featureConfiguration;

    private enum TaskFenzoCheck {AddedToFenzo, EffectivelyFinished, FenzoAddError, Inconsistent}

    static final String BATCH_RESOLVER = "batchResolver";
    static final String SERVICE_RESOLVER = "serviceResolver";

    private static final int MAX_RETRIEVE_TASK_CONCURRENCY = 100;

    private static final JobEventFactory JOB_EVENT_FACTORY = new JobEventFactory();

    private static final Map<Object, Comparator<EntityHolder>> INDEX_COMPARATORS = Collections.singletonMap(
            IndexKind.StatusCreationTime, JobReconciliationFrameworkFactory::compareByStatusCreationTime
    );

    private final DifferenceResolver<JobManagerReconcilerEvent> dispatchingResolver;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final JobStore store;
    private final SchedulingService<? extends TaskRequest> schedulingService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;
    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final EntitySanitizer permissiveEntitySanitizer;
    private final EntitySanitizer strictEntitySanitizer;
    private final InitializationErrorCollector errorCollector; // Keep reference so it is not garbage collected (it holds metrics)
    private final TitusRuntime titusRuntime;
    private final Registry registry;
    private final Clock clock;
    private final Optional<Scheduler> optionalScheduler;

    private final Gauge loadedJobs;
    private final Gauge loadedTasks;
    private final Gauge storeLoadTimeMs;

    @Inject
    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             FeatureActivationConfiguration featureConfiguration,
                                             @Named(BATCH_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             @Named(SERVICE_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService<? extends TaskRequest> schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             SystemSoftConstraint systemSoftConstraint,
                                             SystemHardConstraint systemHardConstraint,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             @Named(JOB_PERMISSIVE_SANITIZER) EntitySanitizer permissiveEntitySanitizer,
                                             @Named(JOB_STRICT_SANITIZER) EntitySanitizer strictEntitySanitizer,
                                             TitusRuntime titusRuntime) {
        this(jobManagerConfiguration, featureConfiguration, batchDifferenceResolver, serviceDifferenceResolver, store,
                schedulingService, capacityGroupService, systemSoftConstraint, systemHardConstraint,
                constraintEvaluatorTransformer, permissiveEntitySanitizer, strictEntitySanitizer, titusRuntime,
                Optional.empty());
    }

    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             FeatureActivationConfiguration featureConfiguration,
                                             DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService<? extends TaskRequest> schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             SystemSoftConstraint systemSoftConstraint,
                                             SystemHardConstraint systemHardConstraint,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             EntitySanitizer permissiveEntitySanitizer,
                                             EntitySanitizer strictEntitySanitizer,
                                             TitusRuntime titusRuntime,
                                             Optional<Scheduler> optionalScheduler) {
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.featureConfiguration = featureConfiguration;
        this.store = store;
        this.schedulingService = schedulingService;
        this.capacityGroupService = capacityGroupService;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.permissiveEntitySanitizer = permissiveEntitySanitizer;
        this.strictEntitySanitizer = strictEntitySanitizer;
        this.optionalScheduler = optionalScheduler;
        this.errorCollector = new InitializationErrorCollector(jobManagerConfiguration, titusRuntime);
        this.titusRuntime = titusRuntime;
        this.registry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();

        this.loadedJobs = registry.gauge(ROOT_METRIC_NAME + "loadedJobs");
        this.loadedTasks = registry.gauge(ROOT_METRIC_NAME + "loadedTasks");
        this.storeLoadTimeMs = registry.gauge(ROOT_METRIC_NAME + "storeLoadTimeMs");

        this.dispatchingResolver = DifferenceResolvers.dispatcher(rootModel -> {
            Job<?> job = rootModel.getEntity();
            JobDescriptor.JobDescriptorExt extensions = job.getJobDescriptor().getExtensions();
            if (extensions instanceof BatchJobExt) {
                return batchDifferenceResolver;
            } else if (extensions instanceof ServiceJobExt) {
                return serviceDifferenceResolver;
            } else {
                throw new IllegalStateException("Unsupported job type " + extensions.getClass());
            }
        });
    }

    ReconciliationFramework<JobManagerReconcilerEvent> newInstance() {
        List<Pair<Job, List<Task>>> jobsAndTasks = checkGlobalConsistency(loadJobsAndTasksFromStore(errorCollector));

        // initialize fenzo with running tasks
        List<InternalReconciliationEngine<JobManagerReconcilerEvent>> engines = new ArrayList<>();
        for (Pair<Job, List<Task>> pair : jobsAndTasks) {
            Job job = pair.getLeft();
            List<Task> tasks = pair.getRight();
            InternalReconciliationEngine<JobManagerReconcilerEvent> engine = newRestoredEngine(job, tasks);
            engines.add(engine);
            for (Task task : tasks) {
                Optional<Task> validatedTask = validateTask(task);
                if (validatedTask.isPresent()) {
                    if (!JobFunctions.isOwnedByKubeScheduler(task)) {
                        TaskFenzoCheck check = addTaskToFenzo(engine, job, task);
                        if (check == TaskFenzoCheck.FenzoAddError) {
                            errorCollector.taskAddToFenzoError(task.getId());
                        } else if (check == TaskFenzoCheck.Inconsistent) {
                            errorCollector.inconsistentTask(task.getId());
                        }
                    }
                } else {
                    errorCollector.invalidTaskRecord(task.getId());
                }
            }
        }

        errorCollector.failIfTooManyBadRecords();

        return new DefaultReconciliationFramework<>(
                engines,
                bootstrapModel -> newEngine(bootstrapModel, true),
                jobManagerConfiguration.getReconcilerIdleTimeoutMs(),
                jobManagerConfiguration.getReconcilerActiveTimeoutMs(),
                INDEX_COMPARATORS,
                registry,
                optionalScheduler
        );
    }

    private InternalReconciliationEngine<JobManagerReconcilerEvent> newRestoredEngine(Job job, List<Task> tasks) {
        EntityHolder jobHolder = EntityHolder.newRoot(job.getId(), job);
        for (Task task : tasks) {
            EntityHolder taskHolder = EntityHolder.newRoot(task.getId(), task);
            EntityHolder decorated = TaskTimeoutChangeActions.setTimeoutOnRestoreFromStore(jobManagerConfiguration, taskHolder, clock);
            jobHolder = jobHolder.addChild(decorated);
        }
        return newEngine(jobHolder, false);
    }

    private InternalReconciliationEngine<JobManagerReconcilerEvent> newEngine(EntityHolder bootstrapModel, boolean newlyCreated) {
        return new DefaultReconciliationEngine<>(bootstrapModel,
                newlyCreated,
                dispatchingResolver,
                INDEX_COMPARATORS,
                JOB_EVENT_FACTORY,
                this::extraChangeActionTags,
                this::extraModelActionTags,
                titusRuntime
        );
    }

    private List<Tag> extraChangeActionTags(ChangeAction changeAction) {
        if (changeAction instanceof TitusChangeAction) {
            TitusChangeAction titusChangeAction = (TitusChangeAction) changeAction;
            return Collections.singletonList(new BasicTag("action", titusChangeAction.getName()));
        }
        return Collections.emptyList();
    }

    private List<Tag> extraModelActionTags(JobManagerReconcilerEvent event) {
        return Collections.singletonList(new BasicTag("event", event.getClass().getSimpleName()));
    }

    /**
     * We need to report three situations here:
     * <ul>
     * <li>task ok in final state, and should not be added to Fenzo</li>
     * <li>task ok, and should not be added to Fenzo</li>
     * <li>task has inconsistent state, and because of that should not be added</li>
     * </ul>
     */
    private TaskFenzoCheck addTaskToFenzo(ReconciliationEngine<JobManagerReconcilerEvent> engine, Job job, Task task) {
        TaskState taskState = task.getStatus().getState();
        if (taskState == TaskState.Accepted) {
            try {
                Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(job, capacityGroupService);
                V3QueueableTask queueableTask = new V3QueueableTask(
                        tierAssignment.getLeft(),
                        tierAssignment.getRight(),
                        job,
                        task,
                        JobFunctions.getJobRuntimePrediction(job),
                        featureConfiguration::isOpportunisticResourcesSchedulingEnabled,
                        () -> JobManagerUtil.filterActiveTaskIds(engine),
                        constraintEvaluatorTransformer,
                        systemSoftConstraint,
                        systemHardConstraint
                );
                schedulingService.addTask(queueableTask);
            } catch (Exception e) {
                logger.error("Failed to add Accepted task to Fenzo queue: {} with error:", task.getId(), e);
                return TaskFenzoCheck.FenzoAddError;
            }
            return TaskFenzoCheck.AddedToFenzo;
        }

        if (isTaskEffectivelyFinished(task)) {
            return TaskFenzoCheck.EffectivelyFinished;
        }

        if (!hasPlacedTaskConsistentState(task)) {
            return TaskFenzoCheck.Inconsistent;
        }

        try {
            Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(job, capacityGroupService);
            schedulingService.addRunningTask(new V3QueueableTask(
                    tierAssignment.getLeft(),
                    tierAssignment.getRight(),
                    job,
                    task,
                    JobFunctions.getJobRuntimePrediction(job),
                    featureConfiguration::isOpportunisticResourcesSchedulingEnabled,
                    JobFunctions.getOpportunisticCpuCount(task).orElse(0),
                    () -> JobManagerUtil.filterActiveTaskIds(engine),
                    constraintEvaluatorTransformer,
                    systemSoftConstraint,
                    systemHardConstraint
            ));
        } catch (Exception e) {
            logger.error("Failed to initialize running task in Fenzo: {} with error:", task.getId(), e);
            return TaskFenzoCheck.FenzoAddError;
        }
        return TaskFenzoCheck.AddedToFenzo;
    }

    /**
     * If the task is in KillInitiated state without resources assigned (this may happen for transition Accepted -> KillInitiated,
     * as we always run through that state), do not add the task to Fenzo, as it was never assigned to any host, and we do
     * not plan to run it. If the task is in Finished state, obviously it should not be added as well.
     */
    private boolean isTaskEffectivelyFinished(Task task) {
        TaskState taskState = task.getStatus().getState();
        return taskState == TaskState.Finished || JobFunctions.containsExactlyTaskStates(task, TaskState.Accepted, TaskState.KillInitiated);
    }

    /**
     * Check if task holds consistent state, and can be added to Fenzo
     */
    private boolean hasPlacedTaskConsistentState(Task task) {
        String host = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
        if (host == null) {
            logger.warn("Task {} in state {} has no host assigned. Ignoring it.", task.getId(), task.getStatus().getState());
            return false;
        }
        return true;
    }

    private List<Pair<Job, List<Task>>> loadJobsAndTasksFromStore(InitializationErrorCollector errorCollector) {
        long startTime = clock.wallTime();

        // load all job/task pairs
        List<Pair<Job, Pair<List<Task>, Integer>>> jobTasksPairs;
        try {
            jobTasksPairs = store.init().andThen(store.retrieveJobs().flatMap(retrievedJobsAndErrors -> {
                errorCollector.corruptedJobRecords(retrievedJobsAndErrors.getRight());

                List<Job<?>> retrievedJobs = retrievedJobsAndErrors.getLeft();
                List<Observable<Pair<Job, Pair<List<Task>, Integer>>>> retrieveTasksObservables = new ArrayList<>();
                for (Job job : retrievedJobs) {
                    // TODO Finished jobs that were not archived immediately should be archived by background archive process
                    if (job.getStatus().getState() == JobState.Finished) {
                        logger.info("Not loading finished job: {}", job.getId());
                        continue;
                    }

                    Optional<Job> validatedJob = validateJob(job);
                    if (validatedJob.isPresent()) {
                        Observable<Pair<Job, Pair<List<Task>, Integer>>> retrieveTasksObservable = store.retrieveTasksForJob(job.getId())
                                .map(taskList -> new Pair<>(validatedJob.get(), taskList));
                        retrieveTasksObservables.add(retrieveTasksObservable);
                    } else {
                        errorCollector.invalidJob(job.getId());
                    }
                }
                return Observable.merge(retrieveTasksObservables, MAX_RETRIEVE_TASK_CONCURRENCY);
            })).toList().toBlocking().singleOrDefault(Collections.emptyList());

            int corruptedTaskRecords = jobTasksPairs.stream().mapToInt(p -> p.getRight().getRight()).sum();
            errorCollector.corruptedTaskRecords(corruptedTaskRecords);

            int taskCount = jobTasksPairs.stream().map(p -> p.getRight().getLeft().size()).reduce(0, (a, v) -> a + v);
            loadedJobs.set(jobTasksPairs.size());
            loadedTasks.set(taskCount);

            for (Pair<Job, Pair<List<Task>, Integer>> jobTaskPair : jobTasksPairs) {
                Job job = jobTaskPair.getLeft();
                List<Task> tasks = jobTaskPair.getRight().getLeft();
                List<String> taskStrings = tasks.stream()
                        .map(t -> String.format("<%s,%s:%s>", t.getId(), JobFunctions.isOwnedByKubeScheduler(t) ? "ks" : "fenzo", t.getStatus().getState()))
                        .collect(Collectors.toList());
                logger.info("Loaded job: {} with tasks: {}", job.getId(), taskStrings);
            }

            logger.info("{} jobs and {} tasks loaded from store in {}ms", jobTasksPairs.size(), taskCount, clock.wallTime() - startTime);
        } catch (Exception e) {
            logger.error("Failed to load jobs from the store during initialization:", e);
            throw new IllegalStateException("Failed to load jobs from the store during initialization", e);
        } finally {
            storeLoadTimeMs.set(clock.wallTime() - startTime);
        }

        return jobTasksPairs.stream().map(p -> Pair.of(p.getLeft(), p.getRight().getLeft())).collect(Collectors.toList());
    }

    private Optional<Job> validateJob(Job job) {
        // Perform strict validation for reporting purposes
        Set<ValidationError> strictViolations = strictEntitySanitizer.validate(job);
        if (!strictViolations.isEmpty()) {
            logger.error("No strictly consistent job record found: jobId={}, violations={}", job.getId(), EntitySanitizerUtil.toStringMap(strictViolations));
            errorCollector.strictlyInvalidJob(job.getId());
        }

        // Required checks
        Set<ValidationError> violations = permissiveEntitySanitizer.validate(job);

        if (!violations.isEmpty()) {
            logger.error("Bad job record found: jobId={}, violations={}", job.getId(), EntitySanitizerUtil.toStringMap(violations));
            if (jobManagerConfiguration.isFailOnDataValidation()) {
                return Optional.empty();
            }
        }

        return Optional.of(job);
    }

    private Optional<Task> validateTask(Task task) {
        // Perform strict validation for reporting purposes
        Set<ValidationError> strictViolations = strictEntitySanitizer.validate(task);
        if (!strictViolations.isEmpty()) {
            logger.error("No strictly consistent task record found: taskId={}, violations={}", task.getId(), EntitySanitizerUtil.toStringMap(strictViolations));
            errorCollector.strictlyInvalidTask(task.getId());
        }

        // Required checks
        Set<ValidationError> violations = permissiveEntitySanitizer.validate(task);

        if (!violations.isEmpty()) {
            logger.error("Bad task record found: taskId={}, violations={}", task.getId(), EntitySanitizerUtil.toStringMap(violations));
            if (jobManagerConfiguration.isFailOnDataValidation()) {
                return Optional.empty();
            }
        }

        return Optional.of(task);
    }

    private List<Pair<Job, List<Task>>> checkGlobalConsistency(List<Pair<Job, List<Task>>> jobsAndTasks) {
        Map<String, Map<String, Set<String>>> eniAssignmentMap = new HashMap<>();

        List<Pair<Job, List<Task>>> filtered = jobsAndTasks.stream()
                .map(jobAndTasks -> {
                            List<Task> filteredTasks = jobAndTasks.getRight().stream()
                                    .map(task -> checkTaskEniAssignment(task, eniAssignmentMap))
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .collect(Collectors.toList());
                            return Pair.of(jobAndTasks.getLeft(), filteredTasks);
                        }
                ).collect(Collectors.toList());

        // Report overlaps
        eniAssignmentMap.forEach((eniSignature, assignments) -> {
            if (assignments.size() > 1) {
                errorCollector.eniOverlaps(eniSignature, assignments);
            }
        });

        return filtered;
    }

    private Optional<Task> checkTaskEniAssignment(Task task, Map<String, Map<String, Set<String>>> eniAssignmentMap) {
        // ENI assignment for tasks managed by Kube scheduler is done by agents.
        if (JobFunctions.isOwnedByKubeScheduler(task)) {
            return Optional.of(task);
        }

        // Filter out tasks that will not be put back into Fenzo queue.
        TaskState taskState = task.getStatus().getState();
        if (taskState == TaskState.Accepted || isTaskEffectivelyFinished(task)) {
            return Optional.of(task);
        }

        // Find agent
        String agent = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
        if (agent == null) {
            errorCollector.launchedTaskWithUnidentifiedAgent(task.getId());
            return Optional.empty();
        }

        // Find ENI assignment
        Optional<TwoLevelResource> eniAssignmentOpt = task.getTwoLevelResources().stream()
                .filter(r -> r.getName().equals("ENIs"))
                .findFirst();
        if (!eniAssignmentOpt.isPresent()) {
            return Optional.of(task);
        }
        TwoLevelResource eniAssignment = eniAssignmentOpt.get();

        // Record
        String eniSignature = "ENI@" + agent + '#' + eniAssignment.getIndex();
        Map<String, Set<String>> eniSGs = eniAssignmentMap.computeIfAbsent(eniSignature, e -> new HashMap<>());
        String normalizedSgs = SecurityGroupUtils.normalizeSecurityGroups(eniAssignment.getValue());
        eniSGs.computeIfAbsent(normalizedSgs, sg -> new HashSet<>()).add(task.getId());

        return eniSGs.size() == 1 ? Optional.of(task) : Optional.empty();
    }

    private static int compareByStatusCreationTime(EntityHolder holder1, EntityHolder holder2) {
        if (holder1.getEntity() instanceof Job) {
            Job job1 = holder1.getEntity();
            Job job2 = holder2.getEntity();
            return Long.compare(job1.getStatus().getTimestamp(), job2.getStatus().getTimestamp());
        }
        Task task1 = holder1.getEntity();
        Task task2 = holder2.getEntity();
        return Long.compare(task1.getStatus().getTimestamp(), task2.getStatus().getTimestamp());
    }
}
