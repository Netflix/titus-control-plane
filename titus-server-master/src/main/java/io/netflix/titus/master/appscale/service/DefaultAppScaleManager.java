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

package io.netflix.titus.master.appscale.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.appscale.model.AutoScalableTarget;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.model.PolicyType;
import io.netflix.titus.api.appscale.service.AppScaleManager;
import io.netflix.titus.api.appscale.service.AutoScalePolicyException;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import io.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import io.netflix.titus.api.connector.cloud.CloudAlarmClient;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.event.JobStateChangeEvent;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.job.service.ServiceJobMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;


@Singleton
public class DefaultAppScaleManager implements AppScaleManager {
    private static Logger log = LoggerFactory.getLogger(DefaultAppScaleManager.class);
    private final AppScaleManagerMetrics metrics;
    private AppScalePolicyStore appScalePolicyStore;
    private final CloudAlarmClient cloudAlarmClient;
    private final AppAutoScalingClient appAutoScalingClient;
    private V2JobOperations v2JobOperations;
    private RxEventBus rxEventBus;
    private V3JobOperations v3JobOperations;
    private Registry registry;

    private volatile Map<String, AutoScalableTarget> scalableTargets;
    private Subscription pendingPoliciesSub;
    private Subscription deletingPoliciesSub;
    private Subscription reconcileFinishedJobsSub;
    private Subscription reconcileScalableTargetsSub;


    public static final String DEFAULT_JOB_GROUP_SEQ = "v000";

    public static class JobScalingConstraints {
        private int minCapacity;
        private int maxCapacity;

        public JobScalingConstraints(int minCapacity, int maxCapacity) {
            this.minCapacity = minCapacity;
            this.maxCapacity = maxCapacity;
        }

        public int getMinCapacity() {
            return minCapacity;
        }

        public int getMaxCapacity() {
            return maxCapacity;
        }
    }

    @Inject
    public DefaultAppScaleManager(AppScalePolicyStore appScalePolicyStore, CloudAlarmClient cloudAlarmClient,
                                  AppAutoScalingClient applicationAutoScalingClient,
                                  V2JobOperations v2JobOperations,
                                  V3JobOperations v3JobOperations,
                                  RxEventBus rxEventBus,
                                  Registry registry) {
        this.appScalePolicyStore = appScalePolicyStore;
        this.cloudAlarmClient = cloudAlarmClient;
        this.appAutoScalingClient = applicationAutoScalingClient;
        this.v2JobOperations = v2JobOperations;
        this.rxEventBus = rxEventBus;
        this.v3JobOperations = v3JobOperations;
        this.registry = registry;
        this.scalableTargets = new ConcurrentHashMap<>();
        this.metrics = new AppScaleManagerMetrics(registry);
    }

    @Activator
    public Completable enterActiveMode() {
        // DB load
        this.appScalePolicyStore.init().await();

        // report metrics from initial DB state
        this.appScalePolicyStore.retrievePolicies(true)
                .map(autoScalingPolicy -> {
                    addScalableTargetIfNew(autoScalingPolicy.getJobId());
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, autoScalingPolicy.getStatus());
                    return autoScalingPolicy.getRefId();
                })
                .subscribe(ignored -> log.info("AppScaleManager store load finished."));

        pendingPoliciesSub = Observable.interval(120, 60, TimeUnit.SECONDS)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> processPendingPolicyRequests())
                .subscribe(policy -> log.info("AutoScalingPolicy {} created ", policy),
                        e -> log.error("Error in processing Pending policy requests", e),
                        () -> log.info("Pending policy processing stream closed ?"));

        deletingPoliciesSub = Observable.interval(140, 60, TimeUnit.SECONDS)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> processDeletingPolicyRequests())
                .subscribe(policy -> log.info("{} policy deleted", policy),
                        e -> log.error("Error in processing Deleting policy requests", e),
                        () -> log.info("Deleting policy processing stream closed ?"));

        reconcileFinishedJobsSub = Observable.interval(ThreadLocalRandom.current().nextInt(10), 15, TimeUnit.MINUTES)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> reconcileFinishedJobs())
                .subscribe(autoScalingPolicy -> log.info("reconciliation (finishedjobs) : autoScalingPolicy {} removed", autoScalingPolicy),
                        e -> log.error("error in reconciliation (finishedjobs) stream", e),
                        () -> log.info("reconciliation (finishedjobs) stream closed"));

        reconcileScalableTargetsSub = Observable.interval(ThreadLocalRandom.current().nextInt(10), 15, TimeUnit.MINUTES)
                .observeOn(Schedulers.io())
                .flatMap(ignored -> reconcileScalableTargets())
                .subscribe(autoScalableTarget -> log.info("Reconciliation (jobStatus) : AutoScalableTarget updated {}", autoScalableTarget),
                        e -> log.error("Error in reconciliation (jobStatus) stream", e),
                        () -> log.info("Reconciliation (jobStatus) stream closed"));


        v2LiveStreamPolicyCleanup()
                .subscribe(autoScalingPolicy -> log.info("(V2) AutoScalingPolicy {} removed", autoScalingPolicy),
                        e -> log.error("Error in V2 job state change event stream", e),
                        () -> log.info("V2 job event stream closed"));

        v2LiveStreamTargetUpdates()
                .subscribe(autoScalableTarget -> log.info("(V2) AutoScalableTarget updated {}", autoScalableTarget),
                        e -> log.error("Error in V2 job state change event stream", e),
                        () -> log.info("V2 job event stream closed"));

        v3LiveStreamTargetUpdates()
                .subscribe(autoScalableTarget -> log.info("(V3) AutoScalableTarget updated {}", autoScalableTarget),
                        e -> log.error("Error in V3 job state change event stream", e),
                        () -> log.info("V3 job event stream closed"));

        v3LiveStreamPolicyCleanup()
                .subscribe(autoScalingPolicy -> log.info("(V3) AutoScalingPolicy {} removed", autoScalingPolicy),
                        e -> log.error("Error in V3 job state change event stream", e),
                        () -> log.info("V3 job event stream closed"));

        return Completable.complete();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(pendingPoliciesSub);
        ObservableExt.safeUnsubscribe(deletingPoliciesSub);
        ObservableExt.safeUnsubscribe(reconcileFinishedJobsSub);
        ObservableExt.safeUnsubscribe(reconcileScalableTargetsSub);
    }


    @VisibleForTesting
    Observable<String> processPendingPolicyRequests() {
        // Create scalable target and policy for all pending policies
        // Return a cached stream so we can multicast the observable for each policy type filter later
        Observable<AutoScalingPolicy> policyObservable = appScalePolicyStore.retrievePolicies(false)
                .filter(autoScalingPolicy -> autoScalingPolicy.getStatus() == PolicyStatus.Pending)
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
                            log.debug("Storing policy ID {} for ref ID {} on Job {}", policyId, autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId());
                            return appScalePolicyStore.updatePolicyId(autoScalingPolicy.getRefId(), policyId)
                                    // Return an observable of the newly update policy
                                    .andThen(Observable.fromCallable(() -> appScalePolicyStore.retrievePolicyForRefId(autoScalingPolicy.getRefId()))
                                            .flatMap(autoScalingPolicyObservable -> autoScalingPolicyObservable));
                        }))
                .cache();

        // Apply TT policies
        Observable<String> targetPolicyObservable = policyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.TargetTrackingScaling)
                .flatMap(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Applied);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Applied)
                            .andThen(Observable.just(autoScalingPolicy.getRefId()));
                });

        // Create alarm and apply SS policies
        Observable<String> stepPolicyObservable = policyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.StepScaling)
                .flatMap(autoScalingPolicy -> {
                    log.debug("Updating alarm for policy {} with Policy ID {}", autoScalingPolicy, autoScalingPolicy.getPolicyId());
                    return cloudAlarmClient.createOrUpdateAlarm(autoScalingPolicy.getRefId(),
                            autoScalingPolicy.getJobId(),
                            autoScalingPolicy.getPolicyConfiguration().getAlarmConfiguration(),
                            buildAutoScalingGroup(autoScalingPolicy.getJobId()),
                            Arrays.asList(autoScalingPolicy.getPolicyId()))
                            .flatMap(alarmId -> appScalePolicyStore.updateAlarmId(autoScalingPolicy.getRefId(), alarmId)
                                    .andThen(Observable.just(autoScalingPolicy)));
                })
                .flatMap(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Applied);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Applied)
                            .andThen(Observable.just(autoScalingPolicy.getRefId()));
                });

        return Observable.mergeDelayError(targetPolicyObservable, stepPolicyObservable)
                .doOnError(e -> log.error("Exception in processPendingPolicyRequests -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }


    @VisibleForTesting
    Observable<String> processDeletingPolicyRequests() {
        // Delete scaling policy for all deleting policies
        // Return a cached stream so we can multicast the observable for each policy type filter later
        Observable<AutoScalingPolicy> deletingPolicyObservable = appScalePolicyStore.retrievePolicies(false)
                .filter(autoScalingPolicy -> autoScalingPolicy.getStatus() == PolicyStatus.Deleting)
                .flatMap(autoScalingPolicy ->
                        appAutoScalingClient.deleteScalingPolicy(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())
                                .andThen(Observable.just(autoScalingPolicy)))
                .cache();

        Observable<AutoScalingPolicy> targetPolicyObservable = deletingPolicyObservable
                .filter(autoScalingPolicy -> autoScalingPolicy.getPolicyConfiguration().getPolicyType() == PolicyType.TargetTrackingScaling);

        Observable<AutoScalingPolicy> stepPolicyObservable = deletingPolicyObservable
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
                .flatMap(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleted);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Deleted)
                            .andThen(Observable.just(autoScalingPolicy.getRefId()));
                })
                .doOnError(e -> log.error("Exception in processDeletingPolicyRequests -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }

    Observable<AutoScalingPolicy> reconcileFinishedJobs() {
        return appScalePolicyStore.retrievePolicies(false)
                .map(autoScalingPolicy -> autoScalingPolicy.getJobId())
                .filter(jobId -> !isJobActive(jobId))
                .flatMap(jobId -> removePoliciesForJob(jobId))
                .doOnError(e -> log.error("Exception in reconcileFinishedJobs -> ", e))
                .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty()));
    }

    Observable<AutoScalableTarget> reconcileScalableTargets() {
        return appScalePolicyStore.retrievePolicies(false)
                .filter(autoScalingPolicy -> isJobActive(autoScalingPolicy.getJobId()))
                .filter(autoScalingPolicy -> {
                    String jobId = autoScalingPolicy.getJobId();
                    return shouldRefreshScalableTargetForJob(jobId, getJobScalingConstraints(autoScalingPolicy.getRefId(),
                            jobId));
                })
                .flatMap(autoScalingPolicy -> updateScalableTargetForJob(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId()))
                .doOnError(e -> log.error("Exception in reconcileScalableTargets -> ", e))
                .onErrorResumeNext(e -> Observable.empty());
    }

    Observable<AutoScalingPolicy> v2LiveStreamPolicyCleanup() {
        return rxEventBus.listen(getClass().getSimpleName(), JobStateChangeEvent.class)
                .flatMap(jobStateChangeEvent -> Observable.just(jobStateChangeEvent)
                        .filter(jse -> jse.getJobState() == JobStateChangeEvent.JobState.Finished)
                        .map(jse -> jse.getJobId())
                        .flatMap(jobId -> removePoliciesForJob(jobId))
                        .doOnError(e -> log.error("Exception in v2LiveStreamPolicyCleanup -> ", e))
                        .onErrorResumeNext(e -> saveStatusOnError(e).andThen(Observable.empty())));

    }

    Observable<AutoScalableTarget> v2LiveStreamTargetUpdates() {
        return rxEventBus.listen(getClass().getSimpleName(), JobStateChangeEvent.class)
                .flatMap(jobStateChangeEvent -> Observable.just(jobStateChangeEvent)
                        .filter(jse -> jse.getJobState() != JobStateChangeEvent.JobState.Finished)
                        .map(jse -> jse.getJobId())
                        .flatMap(jobId -> appScalePolicyStore.retrievePoliciesForJob(jobId))
                        .filter(autoScalingPolicy -> shouldRefreshScalableTargetForJob(autoScalingPolicy.getJobId(),
                                getJobScalingConstraints(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())))
                        .flatMap(autoScalingPolicy -> updateScalableTargetForJob(autoScalingPolicy.getRefId(),
                                autoScalingPolicy.getJobId()))
                        .doOnError(e -> log.error("Exception in v2LiveStreamTargetUpdates -> ", e))
                        .onErrorResumeNext(e -> Observable.empty()));
    }


    Observable<AutoScalableTarget> v3LiveStreamTargetUpdates() {
        return v3JobOperations.observeJobs()
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                        return jobUpdateEvent.getCurrent().getStatus().getState() != JobState.Finished;
                    }
                    return false;
                })
                .cast(JobUpdateEvent.class)
                .flatMap(event -> appScalePolicyStore.retrievePoliciesForJob(event.getCurrent().getId()))
                .filter(autoScalingPolicy -> shouldRefreshScalableTargetForJob(autoScalingPolicy.getJobId(),
                        getJobScalingConstraints(autoScalingPolicy.getRefId(), autoScalingPolicy.getJobId())))
                .flatMap(autoScalingPolicy -> updateScalableTargetForJob(autoScalingPolicy.getRefId(),
                        autoScalingPolicy.getJobId()))
                .doOnError(e -> log.error("Exception in v3LiveStreamTargetUpdates -> ", e))
                .onErrorResumeNext(e -> Observable.empty());
    }

    Observable<AutoScalingPolicy> v3LiveStreamPolicyCleanup() {
        return v3JobOperations.observeJobs()
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                        return jobUpdateEvent.getCurrent().getStatus().getState() != JobState.Finished;
                    }
                    return false;
                })
                .cast(JobUpdateEvent.class)
                .flatMap(event -> removePoliciesForJob(event.getCurrent().getId()))
                .doOnError(e -> log.error("Exception in v3LiveStreamPolicyCleanup -> ", e))
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
                    metrics.reportPolicyStatusTransition(newPolicy, PolicyStatus.Pending);
                    return policyRefId;
                });
    }

    @Override
    public Completable updateAutoScalingPolicy(AutoScalingPolicy autoScalingPolicy) {
        metrics.reportPolicyStatusTransition(autoScalingPolicy, autoScalingPolicy.getStatus());
        return appScalePolicyStore.updatePolicyConfiguration(autoScalingPolicy)
                .andThen(Observable.fromCallable(() -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, autoScalingPolicy.getStatus());
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), autoScalingPolicy.getStatus());
                }).toCompletable());
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

    private Observable<AutoScalableTarget> updateScalableTargetForJob(String policyRefId, String jobId) {
        return Observable.fromCallable(() -> getJobScalingConstraints(policyRefId, jobId))
                .flatMap(jobScalingConstraints ->
                        appAutoScalingClient.createScalableTarget(jobId, jobScalingConstraints.getMinCapacity(), jobScalingConstraints.getMaxCapacity())
                                .andThen(appAutoScalingClient.getScalableTargetsForJob(jobId))
                                .map(autoScalableTarget -> {
                                    scalableTargets.put(jobId, autoScalableTarget);
                                    return autoScalableTarget;
                                }));
    }

    private Observable<AutoScalingPolicy> removePoliciesForJob(String jobId) {
        return appScalePolicyStore.retrievePoliciesForJob(jobId)
                .flatMap(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleting);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Deleting)
                            .andThen(Observable.just(autoScalingPolicy));
                });
    }

    private boolean shouldRefreshScalableTargetForJob(String jobId, JobScalingConstraints jobScalingConstraints) {
        return !scalableTargets.containsKey(jobId) ||
                scalableTargets.get(jobId).getMinCapacity() != jobScalingConstraints.getMinCapacity() ||
                scalableTargets.get(jobId).getMaxCapacity() != jobScalingConstraints.getMaxCapacity();
    }

    private boolean isJobActive(String jobId) {
        if (JobFunctions.isV2JobId(jobId)) {
            return v2JobOperations.getJobMgr(jobId).isActive();
        } else {
            // V3
            return v3JobOperations.getJob(jobId).isPresent();
        }
    }

    @Override
    public Completable removeAutoScalingPolicy(String policyRefId) {
        return appScalePolicyStore.retrievePolicyForRefId(policyRefId)
                .filter(autoScalingPolicy -> autoScalingPolicy.getStatus() != PolicyStatus.Deleted)
                .flatMapCompletable(autoScalingPolicy -> {
                    metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleting);
                    return appScalePolicyStore.updatePolicyStatus(autoScalingPolicy.getRefId(), PolicyStatus.Deleting);
                }).toCompletable();
    }

    private Completable saveStatusOnError(Throwable e) {
        Optional<AutoScalePolicyException> autoScalePolicyExceptionOpt = extractAutoScalePolicyException(e);
        if (!autoScalePolicyExceptionOpt.isPresent()) {
            return Completable.complete();
        }

        AutoScalePolicyException autoScalePolicyException = autoScalePolicyExceptionOpt.get();
        if (autoScalePolicyException.getPolicyRefId() != null && !autoScalePolicyException.getPolicyRefId().isEmpty()) {
            metrics.reportErrorForException(autoScalePolicyException);
            String statusMessage = String.format("%s - %s", autoScalePolicyException.getErrorCode(), autoScalePolicyException.getMessage());
            if (autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.UnknownScalingPolicy ||
                    autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.InvalidScalingPolicy ||
                    autoScalePolicyException.getErrorCode() == AutoScalePolicyException.ErrorCode.JobManagerError) {

                AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy.newBuilder().withRefId(autoScalePolicyException.getPolicyRefId()).build();
                metrics.reportPolicyStatusTransition(autoScalingPolicy, PolicyStatus.Deleted);

                // no need to keep retrying
                return appScalePolicyStore.updateStatusMessage(autoScalePolicyException.getPolicyRefId(), statusMessage)
                        .andThen(appScalePolicyStore.updatePolicyStatus(autoScalePolicyException.getPolicyRefId(), PolicyStatus.Deleted));
            } else {
                return appScalePolicyStore.updateStatusMessage(autoScalePolicyException.getPolicyRefId(), statusMessage);
            }
        } else {
            return Completable.complete();
        }
    }


    private JobScalingConstraints getJobScalingConstraints(String policyRefId, String jobId) {
        if (JobFunctions.isV2JobId(jobId)) {
            V2JobMgrIntf v2JobMgr = v2JobOperations.getJobMgr(jobId);

            if (v2JobMgr == null) {
                throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.jobNotFound(jobId));
            }

            if (!(v2JobMgr instanceof ServiceJobMgr)) {
                throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.notServiceJob(jobId));
            }

            V2JobDefinition jobDefinition = v2JobOperations.getJobMgr(jobId).getJobDefinition();
            StageSchedulingInfo stageSchedulingInfo = jobDefinition.getSchedulingInfo().getStages().values().iterator().next();
            int minCapacity = stageSchedulingInfo.getScalingPolicy().getMin();
            int maxCapacity = stageSchedulingInfo.getScalingPolicy().getMax();
            return new JobScalingConstraints(minCapacity, maxCapacity);
        } else {
            // V3 API
            Optional<Job<?>> job = v3JobOperations.getJob(jobId);
            if (job.isPresent()) {
                if (job.get().getJobDescriptor().getExtensions() instanceof ServiceJobExt) {
                    ServiceJobExt serviceJobExt = (ServiceJobExt) job.get().getJobDescriptor().getExtensions();
                    int minCapacity = serviceJobExt.getCapacity().getMin();
                    int maxCapacity = serviceJobExt.getCapacity().getMax();
                    return new JobScalingConstraints(minCapacity, maxCapacity);
                } else {
                    log.info("Not a service job (V3) {}", jobId);
                    throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.notServiceJob(jobId));
                }
            } else {
                throw AutoScalePolicyException.wrapJobManagerException(policyRefId, JobManagerException.jobNotFound(jobId));
            }
        }
    }

    private String buildAutoScalingGroup(String jobId) {
        String autoScalingGroup;
        if (JobFunctions.isV2JobId(jobId)) {
            V2JobDefinition jobDefinition = v2JobOperations.getJobMgr(jobId).getJobDefinition();
            autoScalingGroup = buildAutoScalingGroupV2(jobDefinition.getParameters());
        } else {
            // assumption - active job
            Job<?> job = v3JobOperations.getJob(jobId).get();
            autoScalingGroup = buildAutoScalingGroupV3(job.getJobDescriptor());
        }
        return autoScalingGroup;
    }

    @VisibleForTesting
    static String buildAutoScalingGroupV2(List<Parameter> jobParameters) {
        String jobGroupSequence = Parameters.getJobGroupSeq(jobParameters) != null ?
                Parameters.getJobGroupSeq(jobParameters) : DEFAULT_JOB_GROUP_SEQ;
        List<String> parameterList = Arrays.asList(Parameters.getAppName(jobParameters),
                Parameters.getJobGroupStack(jobParameters),
                Parameters.getJobGroupDetail(jobParameters),
                jobGroupSequence);
        return buildAutoScalingGroupFromParameters(parameterList);
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

}
