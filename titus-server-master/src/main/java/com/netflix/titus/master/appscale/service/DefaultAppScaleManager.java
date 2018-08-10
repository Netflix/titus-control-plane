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

package com.netflix.titus.master.appscale.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.appscale.model.AutoScalableTarget;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.PolicyStatus;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.service.AppScaleManager;
import com.netflix.titus.api.appscale.service.AutoScalePolicyException;
import com.netflix.titus.api.appscale.store.AppScalePolicyStore;
import com.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import com.netflix.titus.api.connector.cloud.CloudAlarmClient;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;


@Singleton
public class DefaultAppScaleManager implements AppScaleManager {
    public static final long INITIAL_RETRY_DELAY_SEC = 5;
    public static final long MAX_RETRY_DELAY_SEC = 30;
    private static Logger logger = LoggerFactory.getLogger(DefaultAppScaleManager.class);

    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;
    private static final String DEFAULT_JOB_GROUP_SEQ = "v000";
    private static final int ASYNC_HANDLER_BUFFER_CAPACITY = 10_000;

    private final AppScaleManagerMetrics metrics;
    private final SerializedSubject<AppScaleAction, AppScaleAction> appScaleActionsSubject;
    private final AppScalePolicyStore appScalePolicyStore;
    private final CloudAlarmClient cloudAlarmClient;
    private final AppAutoScalingClient appAutoScalingClient;
    private final V3JobOperations v3JobOperations;
    private final AppScaleManagerConfiguration appScaleManagerConfiguration;

    private volatile Map<String, AutoScalableTarget> scalableTargets;
    private volatile Subscription reconcileFinishedJobsSub;
    private volatile Subscription reconcileAllPendingRequests;
    private volatile Subscription reconcileScalableTargetsSub;

    private volatile ExecutorService awsInteractionExecutor;
    private Subscription appScaleActionsSub;

    @Inject
    public DefaultAppScaleManager(AppScalePolicyStore appScalePolicyStore, CloudAlarmClient cloudAlarmClient,
                                  AppAutoScalingClient applicationAutoScalingClient,
                                  V3JobOperations v3JobOperations,
                                  Registry registry,
                                  AppScaleManagerConfiguration appScaleManagerConfiguration) {
        this(appScalePolicyStore, cloudAlarmClient, applicationAutoScalingClient, v3JobOperations,
                registry, appScaleManagerConfiguration,
                ExecutorsExt.namedSingleThreadExecutor("DefaultAppScaleManager"));
    }


    private DefaultAppScaleManager(AppScalePolicyStore appScalePolicyStore, CloudAlarmClient cloudAlarmClient,
                                   AppAutoScalingClient applicationAutoScalingClient,
                                   V3JobOperations v3JobOperations,
                                   Registry registry,
                                   AppScaleManagerConfiguration appScaleManagerConfiguration,
                                   ExecutorService awsInteractionExecutor) {
        this(appScalePolicyStore, cloudAlarmClient, applicationAutoScalingClient, v3JobOperations,
                registry, appScaleManagerConfiguration, Schedulers.from(awsInteractionExecutor));
        this.awsInteractionExecutor = awsInteractionExecutor;
    }

    @VisibleForTesting
    public DefaultAppScaleManager(AppScalePolicyStore appScalePolicyStore, CloudAlarmClient cloudAlarmClient,
                                  AppAutoScalingClient applicationAutoScalingClient,
                                  V3JobOperations v3JobOperations,
                                  Registry registry,
                                  AppScaleManagerConfiguration appScaleManagerConfiguration,
                                  Scheduler awsInteractionScheduler) {
        this.appScalePolicyStore = appScalePolicyStore;
        this.cloudAlarmClient = cloudAlarmClient;
        this.appAutoScalingClient = applicationAutoScalingClient;
        this.v3JobOperations = v3JobOperations;
        this.appScaleManagerConfiguration = appScaleManagerConfiguration;
        this.scalableTargets = new ConcurrentHashMap<>();
        this.metrics = new AppScaleManagerMetrics(registry);
        this.appScaleActionsSubject = PublishSubject.<AppScaleAction>create().toSerialized();

        this.appScaleActionsSub = appScaleActionsSubject
                .onBackpressureDrop(appScaleAction -> {
                    logger.warn("Dropping {}", appScaleAction);
                    metrics.reportDroppedRequest();
                })
                .observeOn(awsInteractionScheduler, ASYNC_HANDLER_BUFFER_CAPACITY)
                .doOnError(e -> logger.error("Exception in appScaleActionsSubject ", e))
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withScheduler(awsInteractionScheduler)
                        .withDelay(INITIAL_RETRY_DELAY_SEC, MAX_RETRY_DELAY_SEC, TimeUnit.SECONDS)
                        .withTitle("Auto-retry for appScaleActionsSubject")
                        .buildExponentialBackoff()
                )
                .subscribe(new AppScaleActionHandler());
    }

    @Activator
    public Completable enterActiveMode() {
        // DB load
        this.appScalePolicyStore.init().await(appScaleManagerConfiguration.getStoreInitTimeoutSeconds(),
                TimeUnit.SECONDS);

        // report metrics from initial DB state
        this.appScalePolicyStore.retrievePolicies(true)
                .map(autoScalingPolicy -> {
                    addScalableTargetIfNew(autoScalingPolicy.getJobId());
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, autoScalingPolicy.getStatus());
                    return autoScalingPolicy.getRefId();
                })
                .subscribe(policyRefId -> logger.debug("AutoScalingPolicy loaded - {}", policyRefId));


        // pending policy creation/updates or deletes
        checkForScalingPolicyActions().toCompletable().await(appScaleManagerConfiguration.getStoreInitTimeoutSeconds(),
                TimeUnit.SECONDS);

        reconcileAllPendingRequests = Observable.interval(
                appScaleManagerConfiguration.getReconcileAllPendingAndDeletingRequestsIntervalMins(), TimeUnit.MINUTES,
                Schedulers.io())
                .flatMap(ignored -> checkForScalingPolicyActions())
                .subscribe(policy -> logger.info("Reconciliation - policy request processed : {}.", policy.getPolicyId()),
                        e -> logger.error("error in reconciliation (ReconcileAllPendingRequests) stream", e),
                        () -> logger.info("reconciliation (ReconcileAllPendingRequests) stream closed"));

        reconcileFinishedJobsSub = Observable.interval(appScaleManagerConfiguration.getReconcileFinishedJobsIntervalMins(), TimeUnit.MINUTES)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> reconcileFinishedJobs())
                .subscribe(jobId -> logger.info("reconciliation for FinishedJob : {} policies cleaned up.", jobId),
                        e -> logger.error("error in reconciliation (FinishedJob) stream", e),
                        () -> logger.info("reconciliation (FinishedJob) stream closed"));

        reconcileScalableTargetsSub = Observable.interval(appScaleManagerConfiguration.getReconcileTargetsIntervalMins(), TimeUnit.MINUTES)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> reconcileScalableTargets())
                .subscribe(jobId -> logger.info("Reconciliation (TargetUpdated) : {} target updated", jobId),
                        e -> logger.error("Error in reconciliation (TargetUpdated) stream", e),
                        () -> logger.info("Reconciliation (TargetUpdated) stream closed"));

        v3LiveStreamTargetUpdates()
                .subscribe(jobId -> logger.info("(V3) Job {} scalable target updated.", jobId),
                        e -> logger.error("Error in V3 job state change event stream", e),
                        () -> logger.info("V3 job event stream closed"));

        v3LiveStreamPolicyCleanup()
                .subscribe(jobId -> logger.info("(V3) Job {} policies clean up.", jobId),
                        e -> logger.error("Error in V3 job state change event stream", e),
                        () -> logger.info("V3 job event stream closed"));

        return Completable.complete();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(reconcileFinishedJobsSub, reconcileScalableTargetsSub,
                reconcileAllPendingRequests, appScaleActionsSub);
        if (awsInteractionExecutor == null) {
            return; // nothing else to do
        }

        // cancel all pending and running tasks
        for (Runnable runnable : awsInteractionExecutor.shutdownNow()) {
            logger.warn("Pending task was halted during shutdown: {}", runnable);
        }
        try {
            boolean terminated = awsInteractionExecutor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!terminated) {
                logger.warn("Not all currently running tasks were terminated");
            }
        } catch (Exception e) {
            logger.error("Shutdown process failed, some tasks may not have been terminated", e);
        }
    }

    private Observable<AutoScalingPolicy> checkForScalingPolicyActions() {
        return appScalePolicyStore.retrievePolicies(false)
                .map(autoScalingPolicy -> {
                    if (autoScalingPolicy.getStatus() == PolicyStatus.Pending) {
                        sendCreatePolicyAction(autoScalingPolicy);
                    } else if (autoScalingPolicy.getStatus() == PolicyStatus.Deleting) {
                        sendDeletePolicyAction(autoScalingPolicy);
                    }
                    return autoScalingPolicy;
                });
    }

    private Observable<String> reconcileFinishedJobs() {
        return appScalePolicyStore.retrievePolicies(false)
                .map(AutoScalingPolicy::getJobId)
                .filter(jobId -> !isJobActive(jobId))
                .flatMap(jobId -> removePoliciesForJob(jobId).andThen(Observable.just(jobId)))
                .doOnError(e -> logger.error("Exception in reconcileFinishedJobs -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }

    private Observable<String> reconcileScalableTargets() {
        return appScalePolicyStore.retrievePolicies(false)
                .filter(autoScalingPolicy -> isJobActive(autoScalingPolicy.getJobId()))
                .filter(autoScalingPolicy -> {
                    String jobId = autoScalingPolicy.getJobId();
                    return shouldRefreshScalableTargetForJob(jobId, getJobScalingConstraints(autoScalingPolicy.getRefId(),
                            jobId));
                })
                .map(this::sendUpdateTargetAction)
                .map(AppScaleAction::getJobId)
                .doOnError(e -> logger.error("Exception in reconcileScalableTargets -> ", e))
                .onErrorResumeNext(e -> Observable.empty());
    }


    Observable<String> v3LiveStreamTargetUpdates() {
        return v3JobOperations.observeJobs()
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                        return jobUpdateEvent.getCurrent().getStatus().getState() != JobState.Finished;
                    }
                    return false;
                })
                .cast(JobUpdateEvent.class)
                .flatMap(event ->
                        appScalePolicyStore.retrievePoliciesForJob(event.getCurrent().getId())
                                .filter(autoScalingPolicy -> shouldRefreshScalableTargetForJob(autoScalingPolicy.getJobId(),
                                        getJobScalingConstraints(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())))
                                .map(this::sendUpdateTargetAction)
                                .map(AppScaleAction::getJobId)
                                .doOnError(e -> logger.error("Exception in v3LiveStreamTargetUpdates -> ", e))
                                .onErrorResumeNext(e -> Observable.empty())
                );
    }

    private Observable<String> v3LiveStreamPolicyCleanup() {
        return v3JobOperations.observeJobs()
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                        return jobUpdateEvent.getCurrent().getStatus().getState() == JobState.Finished;
                    }
                    return false;
                })
                .cast(JobUpdateEvent.class)
                .map(event -> event.getCurrent().getId()) // extract jobId from event
                .flatMap(jobId -> removePoliciesForJob(jobId).andThen(Observable.just(jobId)))
                .doOnError(e -> logger.error("Exception in v3LiveStreamPolicyCleanup -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }

    @Override
    public Observable<String> createAutoScalingPolicy(AutoScalingPolicy autoScalingPolicy) {
        if (autoScalingPolicy.getJobId() == null || autoScalingPolicy.getPolicyConfiguration() == null) {
            return Observable.error(AutoScalePolicyException.invalidScalingPolicy(autoScalingPolicy.getRefId(),
                    String.format("JobID Or PolicyConfiguration missing for %s", autoScalingPolicy.getRefId())));
        }

        return appScalePolicyStore.storePolicy(autoScalingPolicy)
                .map(policyRefId -> {
                    addScalableTargetIfNew(autoScalingPolicy.getJobId());
                    AutoScalingPolicy newPolicy = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).withRefId(policyRefId).build();
                    sendCreatePolicyAction(newPolicy);
                    metrics.reportPolicyStatusTransition(newPolicy, PolicyStatus.Pending);
                    return policyRefId;
                });
    }

    @Override
    public Completable updateAutoScalingPolicy(AutoScalingPolicy autoScalingPolicy) {
        logger.info("Updating AutoScalingPolicy {}", autoScalingPolicy);
        return appScalePolicyStore.retrievePolicyForRefId(autoScalingPolicy.getRefId())
                .map(existingPolicy -> AutoScalingPolicy.newBuilder().withAutoScalingPolicy(existingPolicy)
                        .withPolicyConfiguration(autoScalingPolicy.getPolicyConfiguration()).build())
                .filter(policyWithJobId -> PolicyStateTransitions.isAllowed(policyWithJobId.getStatus(), PolicyStatus.Pending))
                .flatMap(policyWithJobId -> appScalePolicyStore.updatePolicyConfiguration(policyWithJobId).andThen(Observable.just(policyWithJobId)))
                .flatMap(updatedPolicy -> {
                    metrics.reportPolicyStatusTransition(updatedPolicy, PolicyStatus.Pending);
                    return appScalePolicyStore.updatePolicyStatus(updatedPolicy.getRefId(), PolicyStatus.Pending)
                            .andThen(Observable.fromCallable(() -> sendCreatePolicyAction(updatedPolicy)));
                }).toCompletable();
    }

    @Override
    public Observable<AutoScalingPolicy> getScalingPoliciesForJob(String jobId) {
        return appScalePolicyStore.retrievePoliciesForJob(jobId);
    }

    @Override
    public Observable<AutoScalingPolicy> getScalingPolicy(String policyRefId) {
        return appScalePolicyStore.retrievePolicyForRefId(policyRefId);
    }

    @Override
    public Observable<AutoScalingPolicy> getAllScalingPolicies() {
        return appScalePolicyStore.retrievePolicies(false);
    }


    @Override
    public Completable removeAutoScalingPolicy(String policyRefId) {
        return appScalePolicyStore.retrievePolicyForRefId(policyRefId)
                .flatMap(autoScalingPolicy -> {
                    if (PolicyStateTransitions.isAllowed(autoScalingPolicy.getStatus(), PolicyStatus.Deleting)) {
                        logger.info("Removing policy {} for job {}", autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId());
                        metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleting);
                        return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Deleting)
                                .andThen(Observable.fromCallable(() -> sendDeletePolicyAction(autoScalingPolicy)));
                    } else {
                        return Observable.empty();
                    }
                })
                .toCompletable();
    }

    private Completable removePoliciesForJob(String jobId) {
        return appScalePolicyStore.retrievePoliciesForJob(jobId)
                .flatMapCompletable(autoScalingPolicy -> removeAutoScalingPolicy(autoScalingPolicy.getRefId())).toCompletable();
    }

    private boolean shouldRefreshScalableTargetForJob(String jobId, JobScalingConstraints jobScalingConstraints) {
        return !scalableTargets.containsKey(jobId) ||
                scalableTargets.get(jobId).getMinCapacity() != jobScalingConstraints.getMinCapacity() ||
                scalableTargets.get(jobId).getMaxCapacity() != jobScalingConstraints.getMaxCapacity();
    }

    private boolean isJobActive(String jobId) {
        return v3JobOperations.getJob(jobId).isPresent();
    }


    private Completable saveStatusOnError(Throwable e) {
        Optional<AutoScalePolicyException> autoScalePolicyExceptionOpt = extractAutoScalePolicyException(e);
        if (!autoScalePolicyExceptionOpt.isPresent()) {
            return Completable.complete();
        }

        logger.info("Saving AutoScalePolicyException", autoScalePolicyExceptionOpt.get());
        AutoScalePolicyException autoScalePolicyException = autoScalePolicyExceptionOpt.get();
        if (autoScalePolicyException.getPolicyRefId() != null && !autoScalePolicyException.getPolicyRefId().isEmpty()) {
            metrics.reportErrorForException(autoScalePolicyException);
            String statusMessage = String.format("%s - %s", autoScalePolicyException.getErrorCode(), autoScalePolicyException.getMessage());

            AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy.newBuilder().withRefId(autoScalePolicyException.getPolicyRefId()).build();
            if (autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.UnknownScalingPolicy) {
                metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleted);
                return appScalePolicyStore.updateStatusMessage(autoScalePolicyException.getPolicyRefId(), statusMessage)
                        .andThen(appScalePolicyStore.updatePolicyStatus(autoScalePolicyException.getPolicyRefId(), PolicyStatus.Deleted));
            } else if (isPolicyCreationError(autoScalePolicyException)) {
                metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Error);
                return appScalePolicyStore.updateStatusMessage(autoScalePolicyException.getPolicyRefId(), statusMessage)
                        .andThen(appScalePolicyStore.updatePolicyStatus(autoScalePolicyException.getPolicyRefId(), PolicyStatus.Error));
            } else {
                return appScalePolicyStore.updateStatusMessage(autoScalePolicyException.getPolicyRefId(), statusMessage);
            }
        } else {
            return Completable.complete();
        }
    }


    private JobScalingConstraints getJobScalingConstraints(String policyRefId, String jobId) {
        // V3 API
        if (v3JobOperations == null) {
            return new JobScalingConstraints(0, 0);
        }
        Optional<Job<?>> job = v3JobOperations.getJob(jobId);
        if (job.isPresent()) {
            if (job.get().getJobDescriptor().getExtensions() instanceof ServiceJobExt) {
                ServiceJobExt serviceJobExt = (ServiceJobExt) job.get().getJobDescriptor().getExtensions();
                int minCapacity = serviceJobExt.getCapacity().getMin();
                int maxCapacity = serviceJobExt.getCapacity().getMax();
                return new JobScalingConstraints(minCapacity, maxCapacity);
            } else {
                logger.info("Not a service job (V3) {}", jobId);
                throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.notServiceJob(jobId));
            }
        } else {
            throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.jobNotFound(jobId));
        }
    }

    private String buildAutoScalingGroup(String jobId) {
        if (v3JobOperations == null) {
            return jobId;
        }
        return v3JobOperations.getJob(jobId)
                .map(job -> buildAutoScalingGroupV3(job.getJobDescriptor()))
                .orElseThrow(() -> JobManagerException.jobNotFound(jobId));
    }

    @VisibleForTesting
    static String buildAutoScalingGroupV3(JobDescriptor<?> jobDescriptor) {
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        String jobGroupSequence = jobGroupInfo.getSequence() != null ? jobGroupInfo.getSequence() : DEFAULT_JOB_GROUP_SEQ;
        List<String> parameterList = Arrays.asList(jobDescriptor.getApplicationName(), jobGroupInfo.getStack(), jobGroupInfo.getDetail(), jobGroupSequence);
        return buildAutoScalingGroupFromParameters(parameterList);
    }

    private static String buildAutoScalingGroupFromParameters(List<String> parameterList) {
        return parameterList
                .stream()
                .filter(s -> s != null && !s.isEmpty())
                .collect(Collectors.joining("-"));
    }


    private void addScalableTargetIfNew(String jobId) {
        if (!scalableTargets.containsKey(jobId)) {
            metrics.reportNewScalableTarget();
            AutoScalableTarget autoScalableTarget = AutoScalableTarget.newBuilder().build();
            scalableTargets.put(jobId, autoScalableTarget);
        }
    }

    static Optional<AutoScalePolicyException> extractAutoScalePolicyException(Throwable exception) {
        Throwable e = exception;

        while (e != null) {
            if (AutoScalePolicyException.class.isAssignableFrom(e.getClass())) {
                return Optional.of((AutoScalePolicyException) e);
            }
            e = e.getCause();
        }

        return Optional.empty();
    }

    public class AppScaleActionHandler implements Action1<AppScaleAction> {
        @Override
        public void call(AppScaleAction appScaleAction) {
            try {
                switch (appScaleAction.getType()) {
                    case CREATE_SCALING_POLICY:
                        if (appScaleAction.getAutoScalingPolicy().isPresent()) {
                            String policyId = createOrUpdateScalingPolicyWorkflow(appScaleAction.getAutoScalingPolicy().get()).toBlocking().first();
                            logger.info("AutoScalingPolicy {} created/updated", policyId);
                        }
                        break;
                    case DELETE_SCALING_POLICY:
                        if (appScaleAction.getAutoScalingPolicy().isPresent()) {
                            String policyIdDeleted = deleteScalingPolicyWorkflow(appScaleAction.getAutoScalingPolicy().get()).toBlocking().first();
                            logger.info("Autoscaling policy {} deleted", policyIdDeleted);
                        }
                        break;
                    case UPDATE_SCALABLE_TARGET:
                        if (appScaleAction.getPolicyRefId().isPresent()) {
                            logger.info("Asked to remove {}", appScaleAction.getPolicyRefId());
                            AutoScalableTarget updatedTarget = updateScalableTargetWorkflow(appScaleAction.getPolicyRefId().get(), appScaleAction.getJobId()).toBlocking().first();
                            logger.info("AutoScalableTarget updated {}", updatedTarget);
                        }
                        break;
                }
            } catch (Exception ex) {
                logger.error("Exception in processing appScaleAction {}", ex.getMessage());
            }
        }
    }

    private Observable<AutoScalableTarget> updateScalableTargetWorkflow(String policyRefId, String jobId) {
        return Observable.fromCallable(() -> getJobScalingConstraints(policyRefId, jobId))
                .flatMap(jobScalingConstraints ->
                        appAutoScalingClient.createScalableTarget(jobId, jobScalingConstraints.getMinCapacity(), jobScalingConstraints.getMaxCapacity())
                                .andThen(appAutoScalingClient.getScalableTargetsForJob(jobId))
                                .map(autoScalableTarget -> {
                                    scalableTargets.put(jobId, autoScalableTarget);
                                    return autoScalableTarget;
                                }));
    }

    private Observable<String> createOrUpdateScalingPolicyWorkflow(AutoScalingPolicy inputAutoScalingPolicy) {
        Observable<AutoScalingPolicy> cachedPolicyObservable = Observable.just(inputAutoScalingPolicy)
                .flatMap(autoScalingPolicy -> {
                    JobScalingConstraints jobScalingConstraints = getJobScalingConstraints(autoScalingPolicy.getRefId(),
                            autoScalingPolicy.getJobId());
                    return appAutoScalingClient
                            .createScalableTarget(autoScalingPolicy.getJobId(), jobScalingConstraints.getMinCapacity(), jobScalingConstraints.getMaxCapacity())
                            .doOnError(e -> saveStatusOnError(
                                    AutoScalePolicyException.errorCreatingTarget(
                                            autoScalingPolicy.getPolicyId(), autoScalingPolicy.getJobId(), e.getMessage())))
                            .andThen(Observable.just(autoScalingPolicy));
                })
                .flatMap(autoScalingPolicy -> appAutoScalingClient.createOrUpdateScalingPolicy(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId(),
                        autoScalingPolicy.getPolicyConfiguration())
                        .flatMap(policyId -> {
                            logger.debug("Storing policy ID {} for ref ID {} on Job {}", policyId, autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId());
                            return appScalePolicyStore.updatePolicyId(autoScalingPolicy.getRefId(), policyId)
                                    // Return an observable of the newly update policy
                                    .andThen(Observable.fromCallable(() -> appScalePolicyStore.retrievePolicyForRefId(autoScalingPolicy.getRefId()))
                                            .flatMap(autoScalingPolicyObservable -> autoScalingPolicyObservable));
                        }))
                .cache();

        // Apply TT policies
        Observable<String> targetPolicyObservable = cachedPolicyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.TargetTrackingScaling)
                .flatMap(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Applied);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Applied)
                            .andThen(Observable.just(autoScalingPolicy.getRefId()));
                });

        // Create alarm and apply SS policies
        Observable<String> stepPolicyObservable = cachedPolicyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.StepScaling)
                .flatMap(autoScalingPolicy -> {
                    logger.debug("Updating alarm for policy {} with Policy ID {}", autoScalingPolicy, autoScalingPolicy.getPolicyId());
                    return cloudAlarmClient.createOrUpdateAlarm(autoScalingPolicy.getRefId(),
                            autoScalingPolicy.getJobId(),
                            autoScalingPolicy.getPolicyConfiguration().getAlarmConfiguration(),
                            buildAutoScalingGroup(autoScalingPolicy.getJobId()),
                            Collections.singletonList(autoScalingPolicy.getPolicyId())
                    ).flatMap(alarmId -> appScalePolicyStore.updateAlarmId(autoScalingPolicy.getRefId(), alarmId)
                            .andThen(Observable.just(autoScalingPolicy)));
                }).flatMap(autoScalingPolicy -> appScalePolicyStore.retrievePolicyForRefId(autoScalingPolicy.getRefId())
                        .flatMap(latestPolicy -> {
                            if (PolicyStateTransitions.isAllowed(latestPolicy.getStatus(), PolicyStatus.Applied)) {
                                metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Applied);
                                return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Applied)
                                        .andThen(Observable.just(autoScalingPolicy.getRefId()));
                            } else {
                                logger.error("Invalid AutoScaling Policy state - Updating a policy that is either Deleted or Deleting {}",
                                        latestPolicy);
                                return Observable.just(autoScalingPolicy.getRefId());
                            }
                        }));

        return Observable.mergeDelayError(targetPolicyObservable, stepPolicyObservable)
                .doOnError(e -> logger.error("Exception in createOrUpdateScalingPolicyImpl -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }


    private Observable<String> deleteScalingPolicyWorkflow(AutoScalingPolicy policyToBeDeleted) {
        Observable<AutoScalingPolicy> cachedPolicyObservable = Observable.just(policyToBeDeleted)
                .flatMap(autoScalingPolicy ->
                        appAutoScalingClient.deleteScalingPolicy(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())
                                .andThen(Observable.just(autoScalingPolicy)))
                .cache();

        Observable<AutoScalingPolicy> targetPolicyObservable = cachedPolicyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.TargetTrackingScaling);

        Observable<AutoScalingPolicy> stepPolicyObservable = cachedPolicyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.StepScaling)
                .flatMap(autoScalingPolicy -> cloudAlarmClient.deleteAlarm(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())
                        .andThen(Observable.just(autoScalingPolicy)));

        return Observable.mergeDelayError(targetPolicyObservable, stepPolicyObservable)
                .flatMap(autoScalingPolicy -> appScalePolicyStore.retrievePoliciesForJob(autoScalingPolicy.getJobId())
                        .count()
                        .flatMap(c -> {
                            // Last policy - delete target
                            if (c == 1) {
                                return appAutoScalingClient.deleteScalableTarget(autoScalingPolicy.getJobId())
                                        .doOnError(e -> saveStatusOnError(AutoScalePolicyException.errorDeletingTarget(autoScalingPolicy.getPolicyId(),
                                                autoScalingPolicy.getJobId(), e.getMessage())))
                                        .andThen(Observable.just(autoScalingPolicy));
                            } else {
                                return Observable.just(autoScalingPolicy);
                            }
                        }))
                .flatMap(autoScalingPolicy -> appScalePolicyStore.retrievePolicyForRefId(autoScalingPolicy.getRefId())
                        .flatMap(currentPolicy -> {
                            if (PolicyStateTransitions.isAllowed(currentPolicy.getStatus(), PolicyStatus.Deleted)) {
                                metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleted);
                                return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Deleted)
                                        .andThen(Observable.just(autoScalingPolicy.getRefId()));
                            } else {
                                logger.error("Invalid AutoScaling Policy state - Trying to delete a policy {}", currentPolicy);
                                return Observable.just(autoScalingPolicy.getRefId());
                            }
                        }))
                .doOnError(e -> logger.error("Exception in processDeletingPolicyRequests -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }

    private AppScaleAction sendUpdateTargetAction(AutoScalingPolicy autoScalingPolicy) {
        AppScaleAction updateTargetAction = AppScaleAction.newBuilder().buildUpdateTargetAction(autoScalingPolicy.getJobId(), autoScalingPolicy.getRefId());
        appScaleActionsSubject.onNext(updateTargetAction);
        return updateTargetAction;
    }

    private AppScaleAction sendCreatePolicyAction(AutoScalingPolicy autoScalingPolicy) {
        AppScaleAction createPolicyAction = AppScaleAction.newBuilder().buildCreatePolicyAction(autoScalingPolicy.getJobId(), autoScalingPolicy);
        appScaleActionsSubject.onNext(createPolicyAction);
        return createPolicyAction;
    }

    private AppScaleAction sendDeletePolicyAction(AutoScalingPolicy autoScalingPolicy) {
        AppScaleAction deletePolicyAction = AppScaleAction.newBuilder().buildDeletePolicyAction(autoScalingPolicy.getJobId(), autoScalingPolicy);
        appScaleActionsSubject.onNext(deletePolicyAction);
        return deletePolicyAction;
    }

    private Boolean isPolicyCreationError(AutoScalePolicyException autoScalePolicyException) {
        return autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.InvalidScalingPolicy ||
                autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.ErrorCreatingTarget ||
                autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.ErrorCreatingAlarm ||
                autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.ErrorCreatingPolicy ||
                autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.JobManagerError;

    }
}
