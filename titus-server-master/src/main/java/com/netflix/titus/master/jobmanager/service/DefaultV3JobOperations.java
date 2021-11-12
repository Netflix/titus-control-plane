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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobCompatibility;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder.Model;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ProtobufExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import com.netflix.titus.master.jobmanager.service.event.JobCheckpointReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobNewModelReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.jobmanager.service.service.action.BasicServiceJobActions;
import com.netflix.titus.master.jobmanager.service.service.action.MoveTaskBetweenJobsAction;
import com.netflix.titus.master.service.management.ManagementSubsystemInitializer;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.BackpressureOverflow;
import rx.Completable;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.common.util.FunctionExt.alwaysTrue;

@Singleton
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class DefaultV3JobOperations implements V3JobOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultV3JobOperations.class);
    private static final int OBSERVE_JOBS_BACKPRESSURE_BUFFER_SIZE = 1024;

    private static final String METRIC_EVENT_STREAM_LAST_ERROR = MetricConstants.METRIC_ROOT + "jobManager.eventStreamLastError";

    enum IndexKind {StatusCreationTime}

    private static final long RECONCILER_SHUTDOWN_TIMEOUT_MS = 30_000;

    private final JobStore store;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final JobServiceRuntime runtime;
    private final FeatureActivationConfiguration featureActivationConfiguration;
    private final JobReconciliationFrameworkFactory jobReconciliationFrameworkFactory;
    private final JobSubmitLimiter jobSubmitLimiter;
    private final ManagementSubsystemInitializer managementSubsystemInitializer;
    private final TitusRuntime titusRuntime;
    private final EntitySanitizer entitySanitizer;
    private final VersionSupplier versionSupplier;

    private ReconciliationFramework<JobManagerReconcilerEvent> reconciliationFramework;
    private Subscription transactionLoggerSubscription;
    private Subscription reconcilerEventSubscription;

    /**
     * WARNING: we depend here on {@link ManagementSubsystemInitializer} to enforce proper initialization order.
     */
    @Inject
    public DefaultV3JobOperations(JobManagerConfiguration jobManagerConfiguration,
                                  FeatureActivationConfiguration featureActivationConfiguration,
                                  JobStore store,
                                  JobServiceRuntime runtime,
                                  JobReconciliationFrameworkFactory jobReconciliationFrameworkFactory,
                                  JobSubmitLimiter jobSubmitLimiter,
                                  ManagementSubsystemInitializer managementSubsystemInitializer,
                                  TitusRuntime titusRuntime,
                                  @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                  VersionSupplier versionSupplier) {
        this.featureActivationConfiguration = featureActivationConfiguration;
        this.store = store;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.runtime = runtime;
        this.jobReconciliationFrameworkFactory = jobReconciliationFrameworkFactory;
        this.jobSubmitLimiter = jobSubmitLimiter;
        this.managementSubsystemInitializer = managementSubsystemInitializer;
        this.titusRuntime = titusRuntime;
        this.entitySanitizer = entitySanitizer;
        this.versionSupplier = versionSupplier;
    }

    @Activator
    public void enterActiveMode() {
        this.reconciliationFramework = jobReconciliationFrameworkFactory.newInstance();

        // BUG: event stream breaks permanently, and cannot be retried.
        // As we cannot fix the underlying issue yet, we have to be able to discover when it happens.
        AtomicLong eventStreamLastError = new AtomicLong();
        Clock clock = titusRuntime.getClock();
        this.transactionLoggerSubscription = JobTransactionLogger.logEvents(reconciliationFramework, eventStreamLastError, clock);
        PolledMeter.using(titusRuntime.getRegistry())
                .withName(METRIC_EVENT_STREAM_LAST_ERROR)
                .monitorValue(eventStreamLastError, value -> value.get() <= 0 ? 0 : clock.wallTime() - value.get());

        // Remove finished jobs from the reconciliation framework.
        Observable<JobManagerReconcilerEvent> reconciliationEventsObservable = reconciliationFramework.events()
                .onBackpressureBuffer(
                        OBSERVE_JOBS_BACKPRESSURE_BUFFER_SIZE,
                        () -> logger.warn("Overflowed the buffer size: " + OBSERVE_JOBS_BACKPRESSURE_BUFFER_SIZE),
                        BackpressureOverflow.ON_OVERFLOW_ERROR
                ).doOnSubscribe(() -> {
                    List<EntityHolder> entityHolders = reconciliationFramework.orderedView(IndexKind.StatusCreationTime);
                    for (EntityHolder entityHolder : entityHolders) {
                        handleJobCompletedEvent(entityHolder);
                    }
                });
        this.reconcilerEventSubscription = titusRuntime.persistentStream(reconciliationEventsObservable)
                .subscribe(
                        event -> {
                            if (event instanceof JobModelUpdateReconcilerEvent) {
                                JobModelUpdateReconcilerEvent jobUpdateEvent = (JobModelUpdateReconcilerEvent) event;
                                handleJobCompletedEvent(jobUpdateEvent.getChangedEntityHolder());
                            }
                        },
                        e -> logger.error("Event stream terminated with an error", e),
                        () -> logger.info("Event stream completed")
                );

        reconciliationFramework.start();
    }

    private void handleJobCompletedEvent(EntityHolder changedEntityHolder) {
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

    @PreDestroy
    public void shutdown() {
        PolledMeter.remove(titusRuntime.getRegistry(), titusRuntime.getRegistry().createId(METRIC_EVENT_STREAM_LAST_ERROR));
        ObservableExt.safeUnsubscribe(transactionLoggerSubscription, reconcilerEventSubscription);
        if (reconciliationFramework != null) {
            reconciliationFramework.stop(RECONCILER_SHUTDOWN_TIMEOUT_MS);
        }
    }

    @Override
    public Observable<String> createJob(JobDescriptor<?> jobDescriptor, CallMetadata callMetadata) {
        String callerId = callMetadata.getCallers().isEmpty()
                ? "unknown"
                : callMetadata.getCallers().get(0).getId();

        JobDescriptor<?> jobDescriptorWithCallerId = JobFunctions.appendJobDescriptorAttributes(jobDescriptor,
                ImmutableMap.of(JobAttributes.JOB_ATTRIBUTES_CREATED_BY, callerId,
                        JobAttributes.JOB_ATTRIBUTES_CALL_REASON, callMetadata.getCallReason())
        );

        return Observable.fromCallable(() -> jobSubmitLimiter.reserveId(jobDescriptorWithCallerId))
                .flatMap(reservationFailure -> {
                    if (reservationFailure.isPresent()) {
                        return Observable.error(JobManagerException.jobCreateLimited(reservationFailure.get()));
                    }
                    Optional<String> limited = jobSubmitLimiter.checkIfAllowed(jobDescriptorWithCallerId);
                    if (limited.isPresent()) {
                        jobSubmitLimiter.releaseId(jobDescriptorWithCallerId);
                        return Observable.error(JobManagerException.jobCreateLimited(limited.get()));
                    }

                    Job<?> job = newJob(jobDescriptorWithCallerId);
                    String jobId = job.getId();

                    return store.storeJob(job).toObservable()
                            .concatWith(reconciliationFramework.newEngine(EntityHolder.newRoot(jobId, job).addTag(JobManagerConstants.JOB_MANAGER_ATTRIBUTE_CALLMETADATA, callMetadata)))
                            .map(engine -> jobId)
                            .doOnTerminate(() -> jobSubmitLimiter.releaseId(jobDescriptorWithCallerId))
                            .doOnCompleted(() -> logger.info("Created job {} call metadata {}", jobId, callMetadata.getCallerId()))
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
        if (limit <= 0) {
            return Collections.emptyList();
        }

        List<EntityHolder> jobHolders = reconciliationFramework.orderedView(IndexKind.StatusCreationTime);

        List<Job<?>> result = new ArrayList<>();
        int toDrop = offset;
        int toTake = limit;
        for (EntityHolder holder : jobHolders) {
            Pair<Job<?>, List<Task>> jobTasksPair = toJobTasksPair(holder);
            if (queryPredicate.test(jobTasksPair)) {
                if (toDrop > 0) {
                    toDrop--;
                } else {
                    result.add(jobTasksPair.getLeft());
                    toTake--;
                    if (toTake <= 0) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }

        List<EntityHolder> jobHolders = reconciliationFramework.orderedView(IndexKind.StatusCreationTime);
        List<Pair<Job<?>, Task>> result = new ArrayList<>();
        long toDrop = offset;
        long toTake = limit;
        OUTER:
        for (EntityHolder jobHolder : jobHolders) {
            if (!jobHolder.getChildren().isEmpty()) {
                for (EntityHolder taskHolder : jobHolder.getChildren()) {
                    Pair<Job<?>, Task> jobTaskPair = Pair.of(jobHolder.getEntity(), taskHolder.getEntity());
                    if (queryPredicate.test(jobTaskPair)) {
                        if (toDrop > 0) {
                            toDrop--;
                        } else {
                            result.add(jobTaskPair);
                            toTake--;
                            if (toTake <= 0) {
                                break OUTER;
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
        return reconciliationFramework.findEngineByChildId(taskId)
                .map(pair -> {
                    Job<?> job = pair.getLeft().getReferenceView().getEntity();
                    Task task = pair.getRight().getEntity();
                    return Pair.of(job, task);
                });
    }

    @Override
    public Completable updateTask(String taskId, Function<Task, Optional<Task>> changeFunction, Trigger trigger, String reason, CallMetadata callMetadata) {
        Optional<ReconciliationEngine<JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobManagerReconcilerEvent> engine = engineOpt.get();
        TitusChangeAction changeAction = BasicTaskActions.updateTaskInRunningModel(taskId, trigger, jobManagerConfiguration, engine, changeFunction, reason, versionSupplier, titusRuntime, callMetadata);
        return engine.changeReferenceModel(changeAction, taskId).toCompletable();
    }

    @Override
    public Completable recordTaskPlacement(String taskId, Function<Task, Task> changeFunction, CallMetadata callMetadata) {
        Optional<ReconciliationEngine<JobManagerReconcilerEvent>> engineOpt = reconciliationFramework.findEngineByChildId(taskId).map(Pair::getLeft);
        if (!engineOpt.isPresent()) {
            return Completable.error(JobManagerException.taskNotFound(taskId));
        }
        ReconciliationEngine<JobManagerReconcilerEvent> engine = engineOpt.get();

        TitusChangeAction changeAction = TitusChangeAction.newAction("recordTaskPlacement")
                .id(taskId)
                .trigger(Trigger.Scheduler)
                .summary("Scheduler assigned task to an agent")
                .callMetadata(callMetadata)
                .changeWithModelUpdates(self ->
                        JobEntityHolders.expectTask(engine, taskId, titusRuntime)
                                .map(task -> {
                                    Task newTask;
                                    try {
                                        newTask = VersionSuppliers.nextVersion(changeFunction.apply(task), versionSupplier);
                                    } catch (Exception e) {
                                        return Observable.<List<ModelActionHolder>>error(e);
                                    }

                                    TitusModelAction modelUpdate = TitusModelAction.newModelUpdate(self).taskUpdate(newTask);
                                    return store.updateTask(newTask).andThen(Observable.just(ModelActionHolder.allModels(modelUpdate)));
                                })
                                .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(taskId)))
                );
        return engine.changeReferenceModel(changeAction, taskId).toCompletable();
    }

    @Override
    public Observable<Void> updateJobCapacityAttributes(String jobId, CapacityAttributes capacityAttributes, CallMetadata callMetadata) {
        logger.info("UpdateJobCapacityAttributes for job {} - {}", jobId, capacityAttributes);
        String callerId = callMetadata.getCallers().isEmpty()
                ? "unknown"
                : callMetadata.getCallers().get(0).getId();
        updateJobAttributes(jobId, ImmutableMap.of(JobAttributes.JOB_ATTRIBUTES_CREATED_BY, callerId,
                JobAttributes.JOB_ATTRIBUTES_CALL_REASON, callMetadata.getCallReason()), callMetadata);
        return inServiceJob(jobId).flatMap(engine -> engine.changeReferenceModel(
                BasicServiceJobActions.updateJobCapacityAction(engine, capacityAttributes, store, versionSupplier, callMetadata, entitySanitizer)
        ));
    }

    @Override
    public Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata) {
        return inServiceJob(jobId).flatMap(engine -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                        return Observable.error(JobManagerException.notServiceJob(jobId));
                    }
                    return engine.changeReferenceModel(BasicServiceJobActions.updateServiceJobProcesses(engine, serviceJobProcesses, store, versionSupplier, callMetadata));
                }
        );
    }

    @Override
    public Observable<Void> updateJobStatus(String jobId, boolean enabled, CallMetadata callMetadata) {
        return inServiceJob(jobId).flatMap(engine -> {
            Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
            if (serviceJob.getJobDescriptor().getExtensions().isEnabled() == enabled) {
                return Observable.empty();
            }
            return engine.changeReferenceModel(BasicServiceJobActions.updateJobEnableStatus(engine, enabled, store, versionSupplier, callMetadata));
        });
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata) {
        return Mono.fromCallable(() ->
                reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId))
        ).flatMap(engine -> {
            Observable<Void> observableAction = engine.changeReferenceModel(
                    BasicJobActions.updateJobDisruptionBudget(engine, disruptionBudget, store, versionSupplier, callMetadata)
            );
            return ReactorExt.toMono(observableAction);
        });
    }

    @Override
    public Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata) {
        return Mono.fromCallable(() ->
                reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId))
        ).flatMap(engine -> {
            Observable<Void> observableAction = engine.changeReferenceModel(
                    BasicJobActions.updateJobAttributes(engine, attributes, store, versionSupplier, callMetadata)
            );
            return ReactorExt.toMono(observableAction);
        });
    }

    @Override
    public Mono<Void> deleteJobAttributes(String jobId, Set<String> keys, CallMetadata callMetadata) {
        return Mono.fromCallable(() ->
                reconciliationFramework.findEngineByRootId(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId))
        ).flatMap(engine -> {
            Observable<Void> observableAction = engine.changeReferenceModel(
                    BasicJobActions.deleteJobAttributes(engine, keys, store, versionSupplier, callMetadata)
            );
            return ReactorExt.toMono(observableAction);
        });
    }

    @Override
    public Observable<Void> killJob(String jobId, String reason, CallMetadata callMetadata) {
        return reconciliationFramework.findEngineByRootId(jobId)
                .map(engine -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    JobState jobState = job.getStatus().getState();
                    if (jobState == JobState.KillInitiated || jobState == JobState.Finished) {
                        return Observable.<Void>error(JobManagerException.jobTerminating(job));
                    }
                    return engine.changeReferenceModel(KillInitiatedActions.initiateJobKillAction(engine, store, versionSupplier, reason, callMetadata));
                })
                .orElse(Observable.error(JobManagerException.jobNotFound(jobId)));
    }

    @Override
    public Mono<Void> killTask(String taskId, boolean shrink, boolean preventMinSizeUpdate, Trigger trigger, CallMetadata callMetadata) {
        Observable<Void> action = reconciliationFramework.findEngineByChildId(taskId)
                .map(engineChildPair -> {
                    Task task = engineChildPair.getRight().getEntity();
                    TaskState taskState = task.getStatus().getState();
                    if (taskState == TaskState.KillInitiated || taskState == TaskState.Finished) {
                        return Observable.<Void>error(JobManagerException.taskTerminating(task));
                    }

                    String reasonCode;
                    if (trigger == Trigger.Eviction) {
                        reasonCode = TaskStatus.REASON_TASK_EVICTED;
                    } else if (trigger == Trigger.Scheduler) {
                        reasonCode = TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
                    } else {
                        reasonCode = TaskStatus.REASON_TASK_KILLED;
                    }
                    if (shrink) {
                        Job<?> job = engineChildPair.getLeft().getReferenceView().getEntity();
                        if (!(job.getJobDescriptor().getExtensions() instanceof ServiceJobExt)) {
                            return Observable.<Void>error(JobManagerException.notServiceJob(job.getId()));
                        }
                        reasonCode = TaskStatus.REASON_SCALED_DOWN;
                    }
                    String reason = String.format("%s %s(shrink=%s)", Evaluators.getOrDefault(CallMetadataUtils.getFirstCallerId(callMetadata), "<no_caller>"),
                            Evaluators.getOrDefault(callMetadata.getCallReason(), "<no_reason>"), shrink);
                    ChangeAction killAction = KillInitiatedActions.userInitiateTaskKillAction(
                            engineChildPair.getLeft(), runtime, store, versionSupplier, task.getId(), shrink, preventMinSizeUpdate, reasonCode, reason, titusRuntime, callMetadata
                    );
                    return engineChildPair.getLeft().changeReferenceModel(killAction);
                })
                .orElse(Observable.error(JobManagerException.taskNotFound(taskId)));
        return ReactorExt.toMono(action);
    }

    @Override
    public Observable<Void> moveServiceTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata) {
        return Observable.defer(() -> {
            Pair<ReconciliationEngine<JobManagerReconcilerEvent>, EntityHolder> fromEngineTaskPair =
                    reconciliationFramework.findEngineByChildId(taskId).orElseThrow(() -> JobManagerException.taskNotFound(taskId));

            ReconciliationEngine<JobManagerReconcilerEvent> engineFrom = fromEngineTaskPair.getLeft();
            Job<ServiceJobExt> jobFrom = engineFrom.getReferenceView().getEntity();

            if (!JobFunctions.isServiceJob(jobFrom)) {
                throw JobManagerException.notServiceJob(jobFrom.getId());
            }

            if (!jobFrom.getId().equals(sourceJobId)) {
                throw JobManagerException.taskJobMismatch(taskId, sourceJobId);
            }

            if (jobFrom.getId().equals(targetJobId)) {
                throw JobManagerException.sameJobs(jobFrom.getId());
            }

            ReconciliationEngine<JobManagerReconcilerEvent> engineTo =
                    reconciliationFramework.findEngineByRootId(targetJobId).orElseThrow(() -> JobManagerException.jobNotFound(targetJobId));
            Job<ServiceJobExt> jobTo = engineTo.getReferenceView().getEntity();

            if (!JobFunctions.isServiceJob(jobTo)) {
                throw JobManagerException.notServiceJob(jobTo.getId());
            }

            JobCompatibility compatibility = JobCompatibility.of(jobFrom, jobTo);
            if (featureActivationConfiguration.isMoveTaskValidationEnabled() && !compatibility.isCompatible()) {
                Optional<String> diffReport = ProtobufExt.diffReport(
                        GrpcJobManagementModelConverters.toGrpcJobDescriptor(compatibility.getNormalizedDescriptorFrom()),
                        GrpcJobManagementModelConverters.toGrpcJobDescriptor(compatibility.getNormalizedDescriptorTo())
                );
                throw JobManagerException.notCompatible(jobFrom, jobTo, diffReport.orElse(""));
            }

            return reconciliationFramework.changeReferenceModel(
                    new MoveTaskBetweenJobsAction(engineFrom, engineTo, taskId, store, callMetadata, versionSupplier),
                    (rootId, modelUpdatesObservable) -> {
                        String name;
                        String summary;
                        if (targetJobId.equals(rootId)) {
                            name = "moveTask(to)";
                            summary = "Moving a task to this job from job " + jobFrom.getId();
                        } else {
                            name = "moveTask(from)";
                            summary = "Moving a task out of this job to job " + jobTo.getId();
                        }
                        return new TitusChangeAction(Trigger.API, rootId, null, name, summary, callMetadata) {
                            @Override
                            public Observable<List<ModelActionHolder>> apply() {
                                return modelUpdatesObservable;
                            }
                        };
                    },
                    jobFrom.getId(),
                    jobTo.getId()
            );
        });
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                                      Predicate<Pair<Job<?>, Task>> tasksPredicate,
                                                      boolean withCheckpoints) {
        Observable<JobManagerReconcilerEvent> events = reconciliationFramework.events().onBackpressureBuffer(
                OBSERVE_JOBS_BACKPRESSURE_BUFFER_SIZE,
                () -> logger.warn("Overflowed the buffer size: " + OBSERVE_JOBS_BACKPRESSURE_BUFFER_SIZE),
                BackpressureOverflow.ON_OVERFLOW_ERROR
        );
        return toJobManagerEvents(events, jobsPredicate, tasksPredicate, withCheckpoints);
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJob(String jobId) {
        return Observable.fromCallable(() -> reconciliationFramework.findEngineByRootId(jobId))
                .flatMap(engineOpt ->
                        engineOpt.map(engine ->
                                toJobManagerEvents(engine.events(), alwaysTrue(), alwaysTrue(), false)
                        ).orElseGet(() ->
                                Observable.error(JobManagerException.jobNotFound(jobId))
                        ));
    }

    private <E extends JobDescriptor.JobDescriptorExt> Job<E> newJob(JobDescriptor<E> jobDescriptor) {
        String federatedJobId = jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_FEDERATED_JOB_ID);
        String jobId;
        if (StringExt.isNotEmpty(federatedJobId)) {
            jobId = federatedJobId;
            jobDescriptor = JobFunctions.deleteJobAttributes(jobDescriptor, ImmutableSet.of(JobAttributes.JOB_ATTRIBUTES_FEDERATED_JOB_ID));
            jobDescriptor = JobFunctions.appendJobDescriptorAttribute(jobDescriptor, JobAttributes.JOB_ATTRIBUTES_ORIGINAL_FEDERATED_JOB_ID, federatedJobId);
        } else {
            jobId = UUID.randomUUID().toString();
        }

        return Job.<E>newBuilder()
                .withId(jobId)
                .withJobDescriptor(jobDescriptor)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .withReasonMessage("New Job created. Next tasks will be launched.")
                        .build())
                .withVersion(versionSupplier.nextVersion())
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
        List<Task> tasks = new ArrayList<>();
        for (EntityHolder childHolder : jobHolder.getChildren()) {
            tasks.add(childHolder.getEntity());
        }
        return Pair.of(jobHolder.getEntity(), tasks);
    }

    private Observable<JobManagerEvent<?>> toJobManagerEvents(Observable<JobManagerReconcilerEvent> events,
                                                              Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                                              Predicate<Pair<Job<?>, Task>> tasksPredicate,
                                                              boolean withCheckpoints) {
        return events.flatMap(event -> Observable.from(toJobManagerEvent(jobsPredicate, tasksPredicate, withCheckpoints, event)));
    }

    private List<JobManagerEvent<?>> toJobManagerEvent(
            Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
            Predicate<Pair<Job<?>, Task>> tasksPredicate,
            boolean withCheckpoints,
            JobManagerReconcilerEvent event) {
        if (event instanceof JobCheckpointReconcilerEvent) {
            if (withCheckpoints) {
                JobCheckpointReconcilerEvent checkpoint = (JobCheckpointReconcilerEvent) event;
                return Collections.singletonList(JobManagerEvent.keepAliveEvent(checkpoint.getTimestampNano()));
            }
            return Collections.emptyList();
        }
        if (event instanceof JobNewModelReconcilerEvent) {
            JobNewModelReconcilerEvent newModelEvent = (JobNewModelReconcilerEvent) event;
            return toNewJobUpdateEvent(newModelEvent, jobsPredicate);
        }
        if (!(event instanceof JobModelUpdateReconcilerEvent)) {
            return Collections.emptyList();
        }
        JobModelUpdateReconcilerEvent modelUpdateEvent = (JobModelUpdateReconcilerEvent) event;
        if (modelUpdateEvent.getModelActionHolder().getModel() != Model.Reference) {
            return Collections.emptyList();
        }
        if (modelUpdateEvent.getChangedEntityHolder().getEntity() instanceof Job) {
            // A special job event emitted when a task was removed from a job, and a replacement was not created.
            // We have to emit for this case both task archived event followed by job update event.
            if (modelUpdateEvent.getChangeAction().getTrigger() == Trigger.ReconcilerServiceTaskRemoved) {
                Task archivedTask = modelUpdateEvent.getChangeAction().getTask().orElse(null);
                if (archivedTask != null) {
                    Job<?> job = modelUpdateEvent.getJob();
                    TaskUpdateEvent archiveEvent = TaskUpdateEvent.taskArchived(job, archivedTask, modelUpdateEvent.getCallMetadata());
                    List<JobManagerEvent<?>> events = new ArrayList<>();
                    events.add(archiveEvent);
                    events.addAll(toJobUpdateEvent(modelUpdateEvent, jobsPredicate));
                    return events;
                }
            }
            return toJobUpdateEvent(modelUpdateEvent, jobsPredicate);
        }
        return toTaskUpdateEvent(modelUpdateEvent, tasksPredicate);
    }

    private List<JobManagerEvent<?>> toNewJobUpdateEvent(JobNewModelReconcilerEvent newModelEvent,
                                                         Predicate<Pair<Job<?>, List<Task>>> jobsPredicate) {
        Job<?> job = newModelEvent.getNewRoot().getEntity();
        List<Task> tasks = newModelEvent.getNewRoot().getChildren()
                .stream()
                .map(EntityHolder::<Task>getEntity)
                .collect(Collectors.toList());
        return jobsPredicate.test(Pair.of(job, tasks))
                ? Collections.singletonList(JobUpdateEvent.newJob(job, newModelEvent.getCallMetadata()))
                : Collections.emptyList();
    }

    private List<JobManagerEvent<?>> toJobUpdateEvent(JobModelUpdateReconcilerEvent modelUpdateEvent,
                                                      Predicate<Pair<Job<?>, List<Task>>> jobsPredicate) {
        Job<?> changed = modelUpdateEvent.getChangedEntityHolder().getEntity();
        List<Task> tasks = modelUpdateEvent.getChangedEntityHolder().getChildren()
                .stream()
                .map(EntityHolder::<Task>getEntity)
                .collect(Collectors.toList());

        // Archived event is emitted as a very last one, so no need to check for anything extra.
        if (modelUpdateEvent.isArchived()) {
            return jobsPredicate.test(Pair.of(changed, tasks))
                    ? Collections.singletonList(JobUpdateEvent.jobArchived(changed, modelUpdateEvent.getCallMetadata()))
                    : Collections.emptyList();
        }

        if (!modelUpdateEvent.getPreviousEntityHolder().isPresent()) {
            return jobsPredicate.test(Pair.of(changed, tasks))
                    ? Collections.singletonList(JobUpdateEvent.jobChange(changed, changed, modelUpdateEvent.getCallMetadata()))
                    : Collections.emptyList();
        }
        Job<?> previous = modelUpdateEvent.getPreviousEntityHolder().get().getEntity();
        if (changed.equals(previous)) {
            return Collections.emptyList();
        }
        return jobsPredicate.test(Pair.of(changed, tasks))
                ? Collections.singletonList(JobUpdateEvent.jobChange(changed, previous, modelUpdateEvent.getCallMetadata()))
                : Collections.emptyList();
    }

    private List<JobManagerEvent<?>> toTaskUpdateEvent(JobModelUpdateReconcilerEvent modelUpdateEvent,
                                                       Predicate<Pair<Job<?>, Task>> tasksPredicate) {
        Job<?> job = modelUpdateEvent.getJob();
        Task changed = modelUpdateEvent.getChangedEntityHolder().getEntity();

        if (!modelUpdateEvent.getPreviousEntityHolder().isPresent()) {
            if (!tasksPredicate.test(Pair.of(job, changed))) {
                return Collections.emptyList();
            }

            // A task is archived at the same time as its replacement is created. We store the finished task which the
            // new one replaces in the EntityHolder attribute `JobEntityHolders.ATTR_REPLACEMENT_OF`.
            Task archivedTask = (Task) modelUpdateEvent.getChangedEntityHolder().getAttributes().get(JobEntityHolders.ATTR_REPLACEMENT_OF);
            if (archivedTask == null) {
                return Collections.singletonList(toNewTaskUpdateEvent(job, changed, modelUpdateEvent.getCallMetadata()));
            }
            return Arrays.asList(
                    TaskUpdateEvent.taskArchived(job, archivedTask, modelUpdateEvent.getCallMetadata()),
                    toNewTaskUpdateEvent(job, changed, modelUpdateEvent.getCallMetadata())
            );
        }
        Task previous = modelUpdateEvent.getPreviousEntityHolder().get().getEntity();
        if (changed.equals(previous)) {
            return Collections.emptyList();
        }
        return tasksPredicate.test(Pair.of(job, changed))
                ? Collections.singletonList(TaskUpdateEvent.taskChange(job, changed, previous, modelUpdateEvent.getCallMetadata()))
                : Collections.emptyList();
    }

    /**
     * Check if it really is a new task, or if it existed before and was moved from another Job.
     *
     * @return an event indicating if the task was moved from another job
     */
    private TaskUpdateEvent toNewTaskUpdateEvent(Job<?> job, Task newTask, CallMetadata callMetadata) {
        if (newTask.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB)) {
            return TaskUpdateEvent.newTaskFromAnotherJob(job, newTask, callMetadata);
        }
        return TaskUpdateEvent.newTask(job, newTask, callMetadata);
    }
}
