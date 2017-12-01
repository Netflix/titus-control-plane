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

package io.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.DifferenceResolvers;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder.Model;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.internal.DefaultReconciliationFramework;
import io.netflix.titus.common.util.guice.ProxyType;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.event.JobEventFactory;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobNewModelReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.service.action.UpdateJobCapacityAction;
import io.netflix.titus.master.jobmanager.service.service.action.UpdateJobStatusAction;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class DefaultV3JobOperations implements V3JobOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultV3JobOperations.class);

    public static final String BATCH_RESOLVER = "batchResolver";
    public static final String SERVICE_RESOLVER = "serviceResolver";

    private static final long RECONCILER_IDLE_TIMEOUT_MS = 1_000;
    private static final long RECONCILER_ACTIVE_TIMEOUT_MS = 50;
    private static final long RECONCILER_SHUTDOWN_TIMEOUT_MS = 30_000;
    private static final int MAX_RETRIEVE_TASK_CONCURRENCY = 1_000;

    private enum IndexKind {StatusCreationTime}

    private Map<Object, Comparator<EntityHolder>> indexComparators = Collections.singletonMap(
            IndexKind.StatusCreationTime, DefaultV3JobOperations::compareByStatusCreationTime
    );

    private final JobStore store;
    private final VirtualMachineMasterService vmService;
    private final ReconciliationFramework<JobChange, JobManagerReconcilerEvent> reconciliationFramework;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final SchedulingService schedulingService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final Subscription transactionLoggerSubscription;

    @Inject
    public DefaultV3JobOperations(JobManagerConfiguration jobManagerConfiguration,
                                  @Named(BATCH_RESOLVER) DifferenceResolver batchDifferenceResolver,
                                  @Named(SERVICE_RESOLVER) DifferenceResolver serviceDifferenceResolver,
                                  JobStore store,
                                  SchedulingService schedulingService,
                                  VirtualMachineMasterService vmService,
                                  ApplicationSlaManagementService capacityGroupService) {
        this(jobManagerConfiguration, batchDifferenceResolver, serviceDifferenceResolver, store, schedulingService, vmService, capacityGroupService, Schedulers.computation());
    }

    public DefaultV3JobOperations(JobManagerConfiguration jobManagerConfiguration,
                                  @Named(BATCH_RESOLVER) DifferenceResolver batchDifferenceResolver,
                                  @Named(SERVICE_RESOLVER) DifferenceResolver serviceDifferenceResolver,
                                  JobStore store,
                                  SchedulingService schedulingService,
                                  VirtualMachineMasterService vmService,
                                  ApplicationSlaManagementService capacityGroupService,
                                  Scheduler scheduler) {
        this.store = store;
        this.vmService = vmService;
        this.jobManagerConfiguration = jobManagerConfiguration;

        DifferenceResolver dispatchingResolver = DifferenceResolvers.dispatcher(rootModel -> {
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

        this.reconciliationFramework = new DefaultReconciliationFramework<>(
                bootstrapModel -> new DefaultReconciliationEngine<>(bootstrapModel, dispatchingResolver, indexComparators, new JobEventFactory()),
                jobManagerConfiguration.getReconcilerIdleTimeoutMs(),
                jobManagerConfiguration.getReconcilerActiveTimeoutMs(),
                indexComparators,
                scheduler
        );
        this.schedulingService = schedulingService;
        this.capacityGroupService = capacityGroupService;
        this.transactionLoggerSubscription = JobTransactionLogger.logEvents(reconciliationFramework);

        // Remove finished jobs from the reconciliation framework.
        reconciliationFramework.events().subscribe(
                event -> {
                    if (event instanceof JobModelUpdateReconcilerEvent) {
                        JobModelUpdateReconcilerEvent jobUpdateEvent = (JobModelUpdateReconcilerEvent) event;
                        EntityHolder changedEntityHolder = jobUpdateEvent.getChangedEntityHolder();
                        if (changedEntityHolder.getEntity() instanceof Job) {
                            Job<?> job = changedEntityHolder.getEntity();
                            if (job.getStatus().getState() == JobState.Finished) {
                                boolean isClosed = BasicJobActions.isClosed(changedEntityHolder);
                                if (isClosed) {
                                    String jobId = job.getId();
                                    reconciliationFramework.findEngineByRootId(jobId).ifPresent(engine ->
                                            reconciliationFramework.removeEngine(engine).subscribe(
                                                    () -> logger.info("Removed reconciliation engine of job {}", jobId),
                                                    e -> logger.warn("Could not remove reconciliation engine of job {}", jobId, e)
                                            )
                                    );
                                }
                            }
                        }
                    }
                },
                e -> logger.error("Event stream terminated with an error", e),
                () -> logger.info("Event stream completed")
        );
    }

    @Activator
    public void enterActiveMode() {
        // load all job/task pairs
        List<Pair<Job, List<Task>>> pairs;
        try {
            pairs = store.init().andThen(store.retrieveJobs().toList().flatMap(retrievedJobs -> {
                List<Observable<Pair<Job, List<Task>>>> retrieveTasksObservables = new ArrayList<>();
                for (Job job : retrievedJobs) {
                    Observable<Pair<Job, List<Task>>> retrieveTasksObservable = store.retrieveTasksForJob(job.getId())
                            .toList()
                            .map(taskList -> new Pair<>(job, taskList));
                    retrieveTasksObservables.add(retrieveTasksObservable);
                }
                return Observable.merge(retrieveTasksObservables, MAX_RETRIEVE_TASK_CONCURRENCY);
            })).toList().toBlocking().singleOrDefault(Collections.emptyList());
            logger.info("{} jobs loaded from store", pairs.size());
        } catch (Exception e) {
            logger.error("Failed to load jobs from the store during initialization:", e);
            throw new IllegalStateException("Failed to load jobs from the store during initialization", e);
        }

        //TODO sanitize each record to make sure the data is correct
        //TODO sanitize the data based on all records loaded to verify things like unique ENI assignments

        // initialize fenzo with running tasks
        List<String> failedTaskIds = new ArrayList<>();
        for (Pair<Job, List<Task>> pair : pairs) {
            Job job = pair.getLeft();
            for (Task task : pair.getRight()) {
                if (task.getStatus().getState() == TaskState.Accepted) {
                    continue;
                }
                try {
                    Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(job, capacityGroupService);
                    String host = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
                    schedulingService.initRunningTask(new V3QueueableTask(tierAssignment.getLeft(), tierAssignment.getRight(), job, task), host);
                } catch (Exception e) {
                    logger.error("Failed to initialize taskId: {} with error:", task.getId(), e);
                    failedTaskIds.add(task.getId());
                }
            }
        }

        if (!failedTaskIds.isEmpty()) {
            logger.info("Failed to initialize {} tasks with ids: {}", failedTaskIds.size(), failedTaskIds);
        }
        if (failedTaskIds.size() > jobManagerConfiguration.getMaxFailedTasks()) {
            String message = String.format("Exiting because the number of failed tasks was greater than %s", failedTaskIds.size());
            logger.error(message);
            throw new IllegalStateException(message);
        }

        // create entity holders
        List<EntityHolder> entityHolders = new ArrayList<>();
        for (Pair<Job, List<Task>> pair : pairs) {
            Job job = pair.getLeft();
            EntityHolder entityHolder = EntityHolder.newRoot(job.getId(), job);
            for (Task task : pair.getRight()) {
                entityHolder = entityHolder.addChild(EntityHolder.newRoot(task.getId(), task));
            }
            entityHolders.add(entityHolder);
        }

        // create engines and start reconciliation framework
        for (EntityHolder entityHolder : entityHolders) {
            reconciliationFramework.newEngine(entityHolder).subscribe();
        }
        reconciliationFramework.start();
    }

    @PreDestroy
    public void shutdown() {
        transactionLoggerSubscription.unsubscribe();
        reconciliationFramework.stop(RECONCILER_SHUTDOWN_TIMEOUT_MS);
    }

    @Override
    public Observable<String> createJob(JobDescriptor<?> jobDescriptor) {
        return Observable.fromCallable(() -> newJob(jobDescriptor))
                .flatMap(job -> {
                    String jobId = job.getId();
                    return store.storeJob(job).toObservable()
                            .concatWith(reconciliationFramework.newEngine(EntityHolder.newRoot(jobId, job)))
                            .map(engine -> jobId)
                            .doOnCompleted(() -> logger.info("Created job {}", jobId))
                            .doOnError(e -> logger.info("Job {} creation failure", jobId, e));
                });
    }

    @Override
    public List<Job> getJobs() {
        return reconciliationFramework.orderedView(IndexKind.StatusCreationTime).stream()
                .map(entityHolder -> (Job) entityHolder.getEntity())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Job<?>> getJob(String jobId) {
        return reconciliationFramework.findEngineByRootId(jobId).map(engine -> engine.getReferenceView().getEntity());
    }

    @Override
    public List<Task> getTasks() {
        return reconciliationFramework.orderedView(IndexKind.StatusCreationTime).stream()
                .flatMap(entityHolder ->
                        reconciliationFramework.findEngineByRootId(entityHolder.getId())
                                .map(engine -> engine.orderedView(IndexKind.StatusCreationTime).stream().map(h -> (Task) h.getEntity()))
                                .orElse(Stream.empty())
                ).collect(Collectors.toList());
    }

    @Override
    public List<Task> getTasks(String jobId) {
        ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine = reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId));
        return engine.orderedView(IndexKind.StatusCreationTime).stream().map(h -> (Task) h.getEntity()).collect(Collectors.toList());
    }

    @Override
    public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
        List<EntityHolder> jobHolders = reconciliationFramework.orderedView(IndexKind.StatusCreationTime);
        return jobHolders.stream().map(this::toJobTasksPair)
                .filter(queryPredicate)
                .skip(offset)
                .limit(limit)
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    @Override
    public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
        List<EntityHolder> jobHolders = reconciliationFramework.orderedView(IndexKind.StatusCreationTime);
        return jobHolders.stream()
                .filter(jobHolder -> !jobHolder.getChildren().isEmpty())
                .flatMap(jobHolder -> jobHolder.getChildren().stream().map(
                        taskHolder -> Pair.<Job<?>, Task>of(jobHolder.getEntity(), taskHolder.getEntity())
                ))
                .filter(queryPredicate)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public Completable updateTask(String taskId, Function<Task, Task> changeFunction, Trigger trigger, String reason) {
        Optional<ReconciliationEngine<JobChange, JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine = engineOpt.get();
        return engine.changeReferenceModel(BasicTaskActions.updateTaskInRunningModel(taskId, trigger, engine, changeFunction, reason)).toCompletable();
    }

    @Override
    public Completable updateTaskAfterStore(String taskId, Function<Task, Task> changeFunction, Trigger trigger, String reason) {
        Optional<ReconciliationEngine<JobChange, JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine = engineOpt.get();
        return engine.changeReferenceModel(BasicTaskActions.updateTaskAndWriteItToStore(taskId, engine, changeFunction, store, trigger, reason)).toCompletable();
    }

    @Override
    public Observable<Void> updateJobCapacity(String jobId, Capacity capacity) {
        return Observable.fromCallable(() ->
                reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId))
        ).flatMap(engine -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                        return Observable.error(JobManagerException.notServiceJob(jobId));
                    }
                    return engine.changeReferenceModel(new UpdateJobCapacityAction(engine, capacity));
                }
        );
    }

    @Override
    public Observable<Void> updateJobStatus(String serviceJobId, boolean enabled) {
        return reconciliationFramework.findEngineByRootId(serviceJobId)
                .map(engine -> engine.changeReferenceModel(new UpdateJobStatusAction(engine, enabled)))
                .orElse(Observable.error(JobManagerException.jobNotFound(serviceJobId)));
    }

    @Override
    public Observable<Void> killJob(String jobId) {
        return reconciliationFramework.findEngineByRootId(jobId)
                .map(engine -> engine.changeReferenceModel(KillInitiatedActions.initiateJobKillAction(engine, store)))
                .orElse(Observable.error(JobManagerException.jobNotFound(jobId)));
    }

    @Override
    public Observable<Void> killTask(String taskId, boolean shrink, String reason) {
        return reconciliationFramework.findEngineByChildId(taskId)
                .map(engineChildPair -> {
                    if (shrink) {
                        Job<?> job = engineChildPair.getLeft().getReferenceView().getEntity();
                        if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                            return Observable.<Void>error(JobManagerException.notServiceJob(job.getId()));
                        }
                    }
                    Task task = engineChildPair.getRight().getEntity();
                    ChangeAction<JobChange> killAction = KillInitiatedActions.storeAndApplyKillInitiated(
                            engineChildPair.getLeft(), vmService, store, task.getId(), shrink, TaskStatus.REASON_TASK_KILLED, reason
                    );
                    return engineChildPair.getLeft().changeReferenceModel(killAction);
                })
                .orElse(Observable.error(JobManagerException.taskNotFound(taskId)));
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJobs() {
        return toJobManagerEvents(reconciliationFramework.events());
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJob(String jobId) {
        return Observable.fromCallable(() -> reconciliationFramework.findEngineByRootId(jobId))
                .flatMap(engineOpt ->
                        engineOpt.map(engine ->
                                toJobManagerEvents(engine.events())
                        ).orElseGet(() ->
                                Observable.error(JobManagerException.jobNotFound(jobId))
                        ));
    }

    private <E extends JobDescriptor.JobDescriptorExt> Job<E> newJob(JobDescriptor<E> jobDescriptor) {
        return Job.<E>newBuilder()
                .withId(UUID.randomUUID().toString())
                .withJobDescriptor(jobDescriptor)
                .withStatus(JobStatus.newBuilder().withState(JobState.Accepted).build())
                .build();
    }

    private Pair<Job<?>, List<Task>> toJobTasksPair(EntityHolder jobHolder) {
        List<Task> tasks = jobHolder.getChildren().stream().map(childHolder -> (Task) childHolder.getEntity()).collect(Collectors.toList());
        return Pair.of(jobHolder.getEntity(), tasks);
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

    private Observable<JobManagerEvent<?>> toJobManagerEvents(Observable<JobManagerReconcilerEvent> events) {
        return events
                .map(this::toJobManagerEvent)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Optional<JobManagerEvent<?>> toJobManagerEvent(JobManagerReconcilerEvent event) {
        if (event instanceof JobNewModelReconcilerEvent) {
            return Optional.of(JobUpdateEvent.newJob(((JobNewModelReconcilerEvent) event).getNewRoot().getEntity()));
        }
        if (event instanceof JobModelUpdateReconcilerEvent) {
            JobModelUpdateReconcilerEvent modelUpdateEvent = (JobModelUpdateReconcilerEvent) event;
            if (modelUpdateEvent.getModelActionHolder().getModel() != Model.Reference) {
                return Optional.empty();
            }
            if (modelUpdateEvent.getChangedEntityHolder().getEntity() instanceof Job) {
                Job<?> changed = modelUpdateEvent.getChangedEntityHolder().getEntity();
                if (modelUpdateEvent.getPreviousEntityHolder().isPresent()) {
                    Job<?> previous = modelUpdateEvent.getPreviousEntityHolder().get().getEntity();
                    return changed.equals(previous)
                            ? Optional.empty()
                            : Optional.of(JobUpdateEvent.jobChange(changed, previous));
                }
                return Optional.of(JobUpdateEvent.jobChange(changed, changed));
            }
            Job job = modelUpdateEvent.getJob();
            Task changed = modelUpdateEvent.getChangedEntityHolder().getEntity();
            if (!modelUpdateEvent.getPreviousEntityHolder().isPresent()) {
                return Optional.of(TaskUpdateEvent.newTask(job, changed));
            }
            Task previous = modelUpdateEvent.getPreviousEntityHolder().get().getEntity();
            return changed.equals(previous)
                    ? Optional.empty()
                    : Optional.of(TaskUpdateEvent.taskChange(job, changed, previous));
        }
        return Optional.empty();
    }
}
