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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder.Model;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.ProxyType;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobNewModelReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.service.action.BasicServiceJobActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;

import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction.newModelUpdate;

@Singleton
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class DefaultV3JobOperations implements V3JobOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultV3JobOperations.class);

    enum IndexKind {StatusCreationTime}

    private static final long RECONCILER_SHUTDOWN_TIMEOUT_MS = 30_000;

    private final Clock clock;
    private final JobStore store;
    private final VirtualMachineMasterService vmService;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final JobReconciliationFrameworkFactory jobReconciliationFrameworkFactory;

    private ReconciliationFramework<JobManagerReconcilerEvent> reconciliationFramework;
    private Subscription transactionLoggerSubscription;

    private final V3JobMetricsCollector jobMetricsCollector;

    @Inject
    public DefaultV3JobOperations(JobManagerConfiguration jobManagerConfiguration,
                                  JobStore store,
                                  VirtualMachineMasterService vmService,
                                  JobReconciliationFrameworkFactory jobReconciliationFrameworkFactory,
                                  TitusRuntime titusRuntime) {
        this.store = store;
        this.vmService = vmService;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.jobReconciliationFrameworkFactory = jobReconciliationFrameworkFactory;
        this.jobMetricsCollector = new V3JobMetricsCollector(titusRuntime.getRegistry());
        this.clock = titusRuntime.getClock();
    }

    @Activator
    public void enterActiveMode() {
        this.reconciliationFramework = jobReconciliationFrameworkFactory.newInstance();
        this.transactionLoggerSubscription = JobTransactionLogger.logEvents(reconciliationFramework);

        reconciliationFramework.orderedView(IndexKind.StatusCreationTime).forEach(jobHolder -> {
            Job<?> job = jobHolder.getEntity();
            jobHolder.getChildren().forEach(taskHolder -> jobMetricsCollector.updateTaskMetrics(job, taskHolder.getEntity()));
        });

        // Remove finished jobs from the reconciliation framework.
        reconciliationFramework.events().subscribe(
                event -> {
                    if (event instanceof JobModelUpdateReconcilerEvent) {
                        JobModelUpdateReconcilerEvent jobUpdateEvent = (JobModelUpdateReconcilerEvent) event;
                        EntityHolder changedEntityHolder = jobUpdateEvent.getChangedEntityHolder();
                        if (handleJobCompletedEvent(changedEntityHolder)) {
                            jobMetricsCollector.removeJob(changedEntityHolder.getId());
                        } else {
                            jobMetricsCollector.updateTaskMetrics(jobUpdateEvent);
                        }
                    }
                },
                e -> logger.error("Event stream terminated with an error", e),
                () -> logger.info("Event stream completed")
        );

        reconciliationFramework.start();
    }

    private boolean handleJobCompletedEvent(EntityHolder changedEntityHolder) {
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
                    return true;
                }
            }
        }
        return false;
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(transactionLoggerSubscription);
        if (reconciliationFramework != null) {
            reconciliationFramework.stop(RECONCILER_SHUTDOWN_TIMEOUT_MS);
        }
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
        ReconciliationEngine<JobManagerReconcilerEvent> engine = reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId));
        return engine.orderedView(IndexKind.StatusCreationTime).stream().map(h -> (Task) h.getEntity()).collect(Collectors.toList());
    }

    @Override
    public List<Pair<Job, List<Task>>> getJobsAndTasks() {
        return reconciliationFramework.orderedView(IndexKind.StatusCreationTime).stream()
                .map(entityHolder -> {
                    Job job = entityHolder.getEntity();
                    List<Task> tasks = entityHolder.getChildren().stream().map(h -> (Task) h.getEntity()).collect(Collectors.toList());
                    return Pair.of(job, tasks);
                })
                .collect(Collectors.toList());
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
    public Completable updateTask(String taskId, Function<Task, Optional<Task>> changeFunction, Trigger trigger, String reason) {
        Optional<ReconciliationEngine<JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobManagerReconcilerEvent> engine = engineOpt.get();
        return engine.changeReferenceModel(BasicTaskActions.updateTaskInRunningModel(taskId, trigger, jobManagerConfiguration, engine, changeFunction, reason, clock)).toCompletable();
    }

    @Override
    public Completable recordTaskPlacement(String taskId, Function<Task, Task> changeFunction) {
        Optional<ReconciliationEngine<JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobManagerReconcilerEvent> engine = engineOpt.get();

        TitusChangeAction changeAction = TitusChangeAction.newAction("recordTaskPlacement")
                .id(taskId)
                .trigger(Trigger.Scheduler)
                .summary("Scheduler assigned task to an agent")
                .changeWithModelUpdates(self ->
                        JobEntityHolders.expectTask(engine, taskId)
                                .map(task -> {
                                    Task newTask = changeFunction.apply(task);
                                    TitusModelAction modelUpdate = newModelUpdate(self).taskUpdate(newTask);
                                    return store.updateTask(newTask).andThen(Observable.just(ModelActionHolder.allModels(modelUpdate)));
                                })
                                .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(taskId)))
                );
        return engine.changeReferenceModel(changeAction).toCompletable();
    }

    @Override
    public Observable<Void> updateJobCapacity(String jobId, Capacity capacity) {
        return inServiceJob(jobId).flatMap(engine -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    if (serviceJob.getJobDescriptor().getExtensions().getCapacity().equals(capacity)) {
                        return Observable.empty();
                    }
                    if (isDesiredCapacityInvalid(capacity, serviceJob)) {
                        return Observable.error(JobManagerException.invalidDesiredCapacity(jobId, capacity.getDesired(),
                                serviceJob.getJobDescriptor().getExtensions().getServiceJobProcesses()));
                    }
                    return engine.changeReferenceModel(BasicServiceJobActions.updateJobCapacityAction(engine, capacity, store));
                }
        );
    }

    @Override
    public Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses) {
        return inServiceJob(jobId).flatMap(engine -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                        return Observable.error(JobManagerException.notServiceJob(jobId));
                    }
                    return engine.changeReferenceModel(BasicServiceJobActions.updateServiceJobProcesses(engine, serviceJobProcesses, store));
                }
        );
    }

    @Override
    public Observable<Void> updateJobStatus(String jobId, boolean enabled) {
        return inServiceJob(jobId).flatMap(engine -> {
            Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
            if (serviceJob.getJobDescriptor().getExtensions().isEnabled() == enabled) {
                return Observable.empty();
            }
            return engine.changeReferenceModel(BasicServiceJobActions.updateJobEnableStatus(engine, enabled, store));
        });
    }

    @Override
    public Observable<Void> killJob(String jobId) {
        return reconciliationFramework.findEngineByRootId(jobId)
                .map(engine -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    JobState jobState = job.getStatus().getState();
                    if (jobState == JobState.KillInitiated || jobState == JobState.Finished) {
                        return Observable.<Void>error(JobManagerException.jobTerminating(job));
                    }
                    return engine.changeReferenceModel(KillInitiatedActions.initiateJobKillAction(engine, store));
                })
                .orElse(Observable.error(JobManagerException.jobNotFound(jobId)));
    }

    @Override
    public Observable<Void> killTask(String taskId, boolean shrink, String reason) {
        return reconciliationFramework.findEngineByChildId(taskId)
                .map(engineChildPair -> {
                    Task task = engineChildPair.getRight().getEntity();
                    TaskState taskState = task.getStatus().getState();
                    if (taskState == TaskState.KillInitiated || taskState == TaskState.Finished) {
                        return Observable.<Void>error(JobManagerException.taskTerminating(task));
                    }

                    String reasonCode = TaskStatus.REASON_TASK_KILLED;
                    if (shrink) {
                        Job<?> job = engineChildPair.getLeft().getReferenceView().getEntity();
                        if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                            return Observable.<Void>error(JobManagerException.notServiceJob(job.getId()));
                        }
                        reasonCode = TaskStatus.REASON_SCALED_DOWN;
                    }
                    ChangeAction killAction = KillInitiatedActions.userInitiateTaskKillAction(
                            engineChildPair.getLeft(), vmService, store, task.getId(), shrink, reasonCode, String.format("%s (shrink=%s)", reason, shrink)
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

    private Observable<ReconciliationEngine<JobManagerReconcilerEvent>> inServiceJob(String jobId) {
        return Observable.fromCallable(() ->
                reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId))
        ).flatMap(engine -> {
            Job<?> job = engine.getReferenceView().getEntity();
            if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                return Observable.error(JobManagerException.notServiceJob(jobId));
            }
            return Observable.just(engine);
        });
    }

    private Pair<Job<?>, List<Task>> toJobTasksPair(EntityHolder jobHolder) {
        List<Task> tasks = jobHolder.getChildren().stream().map(childHolder -> (Task) childHolder.getEntity()).collect(Collectors.toList());
        return Pair.of(jobHolder.getEntity(), tasks);
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

    private boolean isDesiredCapacityInvalid(Capacity targetCapacity, Job<ServiceJobExt> serviceJob) {
        ServiceJobProcesses serviceJobProcesses = serviceJob.getJobDescriptor().getExtensions().getServiceJobProcesses();
        Capacity currentCapacity = serviceJob.getJobDescriptor().getExtensions().getCapacity();
        return (serviceJobProcesses.isDisableIncreaseDesired() && targetCapacity.getDesired() > currentCapacity.getDesired()) ||
                (serviceJobProcesses.isDisableDecreaseDesired() && targetCapacity.getDesired() < currentCapacity.getDesired());

    }
}
