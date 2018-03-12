package io.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Collection;
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
import javax.validation.ConstraintViolation;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.DifferenceResolvers;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationFramework;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.model.sanitizer.EntitySanitizerUtil;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.jobmanager.service.DefaultV3JobOperations.IndexKind;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions;
import io.netflix.titus.master.jobmanager.service.event.JobEventFactory;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static io.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_SANITIZER;

/**
 * Helper class that encapsulates the creation process of job {@link ReconciliationFramework}.
 */
@Singleton
public class JobReconciliationFrameworkFactory {

    private static final Logger logger = LoggerFactory.getLogger(JobReconciliationFrameworkFactory.class);

    static final String ROOT_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.bootstrap.";

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
    private final SchedulingService schedulingService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;
    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final EntitySanitizer entitySanitizer;
    private final InitializationErrorCollector errorCollector; // Keep reference so it is not garbage collected (it holds metrics)
    private final Registry registry;
    private final Clock clock;
    private final Scheduler scheduler;

    private final Gauge loadedJobs;
    private final Gauge loadedTasks;
    private final Gauge storeLoadTimeMs;

    @Inject
    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             @Named(BATCH_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             @Named(SERVICE_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             SystemSoftConstraint systemSoftConstraint,
                                             SystemHardConstraint systemHardConstraint,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             @Named(JOB_SANITIZER) EntitySanitizer entitySanitizer,
                                             Registry registry) {
        this(jobManagerConfiguration, batchDifferenceResolver, serviceDifferenceResolver, store, schedulingService, capacityGroupService,
                systemSoftConstraint, systemHardConstraint, constraintEvaluatorTransformer, entitySanitizer, registry, Clocks.system(), Schedulers.computation());
    }

    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             @Named(BATCH_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             @Named(SERVICE_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             SystemSoftConstraint systemSoftConstraint,
                                             SystemHardConstraint systemHardConstraint,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             EntitySanitizer entitySanitizer,
                                             Registry registry,
                                             Clock clock,
                                             Scheduler scheduler) {
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.store = store;
        this.schedulingService = schedulingService;
        this.capacityGroupService = capacityGroupService;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.entitySanitizer = entitySanitizer;
        this.errorCollector = new InitializationErrorCollector(jobManagerConfiguration, registry);
        this.registry = registry;
        this.clock = clock;
        this.scheduler = scheduler;

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
        List<ReconciliationEngine<JobManagerReconcilerEvent>> engines = new ArrayList<>();
        for (Pair<Job, List<Task>> pair : jobsAndTasks) {
            Job job = pair.getLeft();
            List<Task> tasks = pair.getRight();
            ReconciliationEngine<JobManagerReconcilerEvent> engine = newRestoredEngine(job, tasks);
            engines.add(engine);
            for (Task task : tasks) {
                Optional<Task> validatedTask = validateTask(task);
                if (validatedTask.isPresent()) {
                    TaskFenzoCheck check = addTaskToFenzo(engine, job, task);
                    if (check == TaskFenzoCheck.FenzoAddError) {
                        errorCollector.taskAddToFenzoError(task.getId());
                    } else if (check == TaskFenzoCheck.Inconsistent) {
                        errorCollector.inconsistentTask(task.getId());
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
                scheduler
        );
    }

    private ReconciliationEngine<JobManagerReconcilerEvent> newRestoredEngine(Job job, List<Task> tasks) {
        EntityHolder jobHolder = EntityHolder.newRoot(job.getId(), job);
        for (Task task : tasks) {
            EntityHolder taskHolder = EntityHolder.newRoot(task.getId(), task);
            EntityHolder decorated = TaskTimeoutChangeActions.setTimeoutOnRestoreFromStore(jobManagerConfiguration, taskHolder, clock);
            jobHolder = jobHolder.addChild(decorated);
        }
        return newEngine(jobHolder, false);
    }

    private ReconciliationEngine<JobManagerReconcilerEvent> newEngine(EntityHolder bootstrapModel, boolean newlyCreated) {
        return new DefaultReconciliationEngine<>(bootstrapModel,
                newlyCreated,
                dispatchingResolver,
                INDEX_COMPARATORS,
                JOB_EVENT_FACTORY,
                this::extraChangeActionTags,
                this::extraModelActionTags,
                registry,
                clock
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
                        () -> JobManagerUtil.filterActiveTaskIds(engine),
                        constraintEvaluatorTransformer,
                        systemSoftConstraint,
                        systemHardConstraint
                );
                schedulingService.getTaskQueueAction().call(queueableTask);
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
            String host = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
            schedulingService.initRunningTask(new V3QueueableTask(
                    tierAssignment.getLeft(),
                    tierAssignment.getRight(),
                    job,
                    task,
                    () -> JobManagerUtil.filterActiveTaskIds(engine),
                    constraintEvaluatorTransformer,
                    systemSoftConstraint,
                    systemHardConstraint
            ), host);
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

                    // TODO Finished jobs that were not archived immediately should be moved by background archive process
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
        Set<ConstraintViolation<Job>> violations = entitySanitizer.validate(job);

        if (!violations.isEmpty()) {
            logger.error("Bad job record found: jobId={}, violations={}", job.getId(), EntitySanitizerUtil.toStringMap((Collection) violations));
            if(jobManagerConfiguration.isFailOnDataValidation()) {
                return Optional.empty();
            }
        }

        return Optional.of(job);
    }

    private Optional<Task> validateTask(Task task) {
        Set<ConstraintViolation<Task>> violations = entitySanitizer.validate(task);

        if (!violations.isEmpty()) {
            logger.error("Bad task record found: taskId={}, violations={}", task.getId(), EntitySanitizerUtil.toStringMap((Collection) violations));
            if(jobManagerConfiguration.isFailOnDataValidation()) {
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
        eniSGs.computeIfAbsent(eniAssignment.getValue(), sg -> new HashSet<>()).add(task.getId());

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
