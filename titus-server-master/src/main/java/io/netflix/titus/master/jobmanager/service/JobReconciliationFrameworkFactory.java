package io.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
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
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.jobmanager.service.DefaultV3JobOperations.IndexKind;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.event.JobEventFactory;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.scheduler.constraint.GlobalConstraintEvaluator;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Helper class that encapsulates the creation process of job {@link ReconciliationFramework}.
 * TODO sanitize each record to make sure the data is correct
 * TODO sanitize the data based on all records loaded to verify things like unique ENI assignments
 */
@Singleton
public class JobReconciliationFrameworkFactory {

    private static final Logger logger = LoggerFactory.getLogger(JobReconciliationFrameworkFactory.class);

    private static final String ROOT_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.bootstrap.";

    static final String BATCH_RESOLVER = "batchResolver";
    static final String SERVICE_RESOLVER = "serviceResolver";

    private static final int MAX_RETRIEVE_TASK_CONCURRENCY = 1_000;

    private static final JobEventFactory JOB_EVENT_FACTORY = new JobEventFactory();

    private static final Map<Object, Comparator<EntityHolder>> INDEX_COMPARATORS = Collections.singletonMap(
            IndexKind.StatusCreationTime, JobReconciliationFrameworkFactory::compareByStatusCreationTime
    );

    private final DifferenceResolver<JobManagerReconcilerEvent> dispatchingResolver;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final JobStore store;
    private final SchedulingService schedulingService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final GlobalConstraintEvaluator globalConstraintEvaluator;
    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer;
    private final Registry registry;
    private final Clock clock;
    private final Scheduler scheduler;

    private final Gauge loadedJobs;
    private final Gauge loadedTasks;
    private final Gauge storeLoadTimeMs;
    private final Gauge badTasks;

    @Inject
    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             @Named(BATCH_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             @Named(SERVICE_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             GlobalConstraintEvaluator globalConstraintEvaluator,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             Registry registry) {
        this(jobManagerConfiguration, batchDifferenceResolver, serviceDifferenceResolver, store, schedulingService, capacityGroupService, globalConstraintEvaluator, constraintEvaluatorTransformer, registry, Clocks.system(), Schedulers.computation());
    }

    public JobReconciliationFrameworkFactory(JobManagerConfiguration jobManagerConfiguration,
                                             @Named(BATCH_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> batchDifferenceResolver,
                                             @Named(SERVICE_RESOLVER) DifferenceResolver<JobManagerReconcilerEvent> serviceDifferenceResolver,
                                             JobStore store,
                                             SchedulingService schedulingService,
                                             ApplicationSlaManagementService capacityGroupService,
                                             GlobalConstraintEvaluator globalConstraintEvaluator,
                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                             Registry registry,
                                             Clock clock,
                                             Scheduler scheduler) {
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.store = store;
        this.schedulingService = schedulingService;
        this.capacityGroupService = capacityGroupService;
        this.globalConstraintEvaluator = globalConstraintEvaluator;
        this.constraintEvaluatorTransformer = constraintEvaluatorTransformer;
        this.registry = registry;
        this.clock = clock;
        this.scheduler = scheduler;

        this.loadedJobs = registry.gauge(ROOT_METRIC_NAME + "loadedJobs");
        this.loadedTasks = registry.gauge(ROOT_METRIC_NAME + "loadedTasks");
        this.storeLoadTimeMs = registry.gauge(ROOT_METRIC_NAME + "storeLoadTimeMs");
        this.badTasks = registry.gauge(ROOT_METRIC_NAME + "badTasks");

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
        List<Pair<Job, List<Task>>> jobsAndTasks = loadJobsAndTasksFromStore();

        // initialize fenzo with running tasks
        List<ReconciliationEngine<JobManagerReconcilerEvent>> engines = new ArrayList<>();
        List<String> failedTaskIds = new ArrayList<>();
        for (Pair<Job, List<Task>> pair : jobsAndTasks) {
            Job job = pair.getLeft();
            List<Task> tasks = pair.getRight();
            ReconciliationEngine<JobManagerReconcilerEvent> engine = newEngine(job, tasks);
            engines.add(engine);
            for (Task task : tasks) {
                addTaskToFenzo(engine, job, task).ifPresent(error -> failedTaskIds.add(task.getId()));
            }
        }

        badTasks.set(failedTaskIds.size());

        if (!failedTaskIds.isEmpty()) {
            logger.info("Failed to initialize {} tasks with ids: {}", failedTaskIds.size(), failedTaskIds);
        }
        if (failedTaskIds.size() > jobManagerConfiguration.getMaxFailedTasks()) {
            String message = String.format("Exiting because the number of failed tasks was greater than %s", failedTaskIds.size());
            logger.error(message);
            throw new IllegalStateException(message);
        }

        return new DefaultReconciliationFramework<>(
                engines,
                this::newEngine,
                jobManagerConfiguration.getReconcilerIdleTimeoutMs(),
                jobManagerConfiguration.getReconcilerActiveTimeoutMs(),
                INDEX_COMPARATORS,
                registry,
                scheduler
        );
    }

    private ReconciliationEngine<JobManagerReconcilerEvent> newEngine(Job job, List<Task> tasks) {
        EntityHolder jobHolder = EntityHolder.newRoot(job.getId(), job);
        for (Task task : tasks) {
            jobHolder = jobHolder.addChild(EntityHolder.newRoot(task.getId(), task));
        }
        return newEngine(jobHolder);
    }

    private ReconciliationEngine<JobManagerReconcilerEvent> newEngine(EntityHolder bootstrapModel) {
        return new DefaultReconciliationEngine<>(bootstrapModel,
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

    private Optional<Throwable> addTaskToFenzo(ReconciliationEngine<JobManagerReconcilerEvent> engine, Job job, Task task) {
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
                        globalConstraintEvaluator
                );
                schedulingService.getTaskQueueAction().call(queueableTask);
            } catch (Exception e) {
                logger.error("Failed to add Accepted task to Fenzo queue: {} with error:", task.getId(), e);
                return Optional.of(e);
            }
        } else if (taskState != TaskState.Finished) {
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
                        globalConstraintEvaluator
                ), host);
            } catch (Exception e) {
                logger.error("Failed to initialize running task in Fenzo: {} with error:", task.getId(), e);
                return Optional.of(e);
            }
        }
        return Optional.empty();
    }

    private List<Pair<Job, List<Task>>> loadJobsAndTasksFromStore() {
        long startTime = clock.wallTime();

        // load all job/task pairs
        List<Pair<Job, List<Task>>> pairs;
        try {
            pairs = store.init().andThen(store.retrieveJobs().toList().flatMap(retrievedJobs -> {
                List<Observable<Pair<Job, List<Task>>>> retrieveTasksObservables = new ArrayList<>();
                for (Job job : retrievedJobs) {
                    // TODO Finished jobs that were not archived immediately should be moved by background archive process
                    if (job.getStatus().getState() != JobState.Finished) {
                        Observable<Pair<Job, List<Task>>> retrieveTasksObservable = store.retrieveTasksForJob(job.getId())
                                .toList()
                                .map(taskList -> new Pair<>(job, taskList));
                        retrieveTasksObservables.add(retrieveTasksObservable);
                    }
                }
                return Observable.merge(retrieveTasksObservables, MAX_RETRIEVE_TASK_CONCURRENCY);
            })).toList().toBlocking().singleOrDefault(Collections.emptyList());

            int taskCount = pairs.stream().map(p -> p.getRight().size()).reduce(0, (a, v) -> a + v);
            loadedJobs.set(pairs.size());
            loadedTasks.set(taskCount);

            logger.info("{} jobs and {} tasks loaded from store in {}ms", pairs.size(), taskCount, clock.wallTime() - startTime);
        } catch (Exception e) {
            logger.error("Failed to load jobs from the store during initialization:", e);
            throw new IllegalStateException("Failed to load jobs from the store during initialization", e);
        } finally {
            storeLoadTimeMs.set(clock.wallTime() - startTime);
        }
        return pairs;
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
