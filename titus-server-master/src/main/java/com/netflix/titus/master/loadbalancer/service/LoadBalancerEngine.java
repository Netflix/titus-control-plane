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

package com.netflix.titus.master.loadbalancer.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import com.netflix.titus.api.loadbalancer.model.TargetState;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.batch.Batch;
import com.netflix.titus.common.util.rx.batch.LargestPerTimeBucket;
import com.netflix.titus.common.util.rx.batch.Priority;
import com.netflix.titus.common.util.rx.batch.RateLimitedBatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static com.netflix.titus.master.MetricConstants.METRIC_LOADBALANCER;

class LoadBalancerEngine {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerEngine.class);

    private static final String METRIC_BATCHES = METRIC_LOADBALANCER + "batches";
    private static final String METRIC_BATCHER = METRIC_LOADBALANCER + "batcher";

    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

    private final TitusRuntime titusRuntime;
    private final CodeInvariants invariants;
    private final LoadBalancerConfiguration configuration;
    private final LoadBalancerJobOperations jobOperations;
    private final TokenBucket connectorTokenBucket;
    private final LoadBalancerConnector connector;
    private final LoadBalancerStore store;
    private final LoadBalancerReconciler reconciler;
    private final Scheduler scheduler;

    LoadBalancerEngine(TitusRuntime titusRuntime,
                       LoadBalancerConfiguration configuration,
                       LoadBalancerJobOperations loadBalancerJobOperations,
                       LoadBalancerReconciler reconciler,
                       LoadBalancerConnector loadBalancerConnector,
                       LoadBalancerStore loadBalancerStore,
                       TokenBucket connectorTokenBucket,
                       Scheduler scheduler) {
        this.titusRuntime = titusRuntime;
        this.invariants = titusRuntime.getCodeInvariants();
        this.configuration = configuration;
        this.jobOperations = loadBalancerJobOperations;
        this.connector = loadBalancerConnector;
        this.store = loadBalancerStore;
        this.connectorTokenBucket = connectorTokenBucket;
        this.reconciler = reconciler;
        this.scheduler = scheduler;
    }

    // TODO(Andrew L): Method does not need to be Rx.
    public Completable add(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> pendingAssociations.onNext(jobLoadBalancer));
    }

    // TODO(Andrew L): Method does not need to be Rx.
    public Completable remove(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> pendingDissociations.onNext(jobLoadBalancer));
    }

    Observable<Batch<TargetStateBatchable, String>> events() {
        // the reconciliation loop must not stop the rest of the rx pipeline onErrors, so we retry
        Observable<TargetStateBatchable> reconcilerEvents = titusRuntime.persistentStream(
                reconciler.events().doOnError(e -> logger.error("Reconciliation error", e))
        );

        Observable<TaskUpdateEvent> taskEvents = jobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class);
        Observable<TaskUpdateEvent> stateTransitions = taskEvents.filter(TaskHelpers::isStateTransition);
        Observable<TaskUpdateEvent> tasksMoved = taskEvents.filter(TaskUpdateEvent::isMovedFromAnotherJob);

        final Observable<TargetStateBatchable> updates = Observable.merge(
                reconcilerEvents,
                pendingAssociations.compose(targetsForJobLoadBalancers(State.Registered)),
                pendingDissociations.compose(targetsForJobLoadBalancers(State.Deregistered)),
                registerFromEvents(stateTransitions),
                deregisterFromEvents(stateTransitions),
                moveFromEvents(tasksMoved)
        ).compose(disableReconciliationTemporarily());

        return updates
                .compose(ObservableExt.batchWithRateLimit(buildBatcher(), METRIC_BATCHES, titusRuntime.getRegistry()))
                .filter(batch -> !batch.getItems().isEmpty())
                .onBackpressureDrop(batch -> logger.warn("Backpressure! Dropping batch for {} size {}", batch.getIndex(), batch.size()))
                .doOnNext(batch -> logger.debug("Processing batch for {} size {}", batch.getIndex(), batch.size()))
                .flatMap(this::applyUpdates)
                .doOnNext(batch -> logger.info("Processed {} load balancer updates for {}", batch.size(), batch.getIndex()))
                .doOnError(e -> logger.error("Error batching load balancer calls", e))
                .retry();
    }

    /**
     * Prevent reconciliation from undoing in-flight updates, since it will be running off cached (and potentially
     * stale) state.
     */
    private Observable.Transformer<TargetStateBatchable, TargetStateBatchable> disableReconciliationTemporarily() {
        return updates -> updates.doOnNext(update ->
                reconciler.activateCooldownFor(update.getIdentifier(), configuration.getCooldownPeriodMs(), TimeUnit.MILLISECONDS)
        );
    }

    public void shutdown() {
        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }

    private Observable<Batch<TargetStateBatchable, String>> applyUpdates(Batch<TargetStateBatchable, String> batch) {
        final String loadBalancerId = batch.getIndex();
        final Map<State, List<TargetStateBatchable>> byState = batch.getItems().stream()
                .collect(Collectors.groupingBy(TargetStateBatchable::getState));

        final Completable registerAll = CollectionsExt.optionalOfNotEmpty(byState.get(State.Registered))
                .map(TaskHelpers::ipAddresses)
                .map(ipAddresses -> connector.registerAll(loadBalancerId, ipAddresses))
                .orElse(Completable.complete());

        final Completable deregisterAll = CollectionsExt.optionalOfNotEmpty(byState.get(State.Deregistered))
                .map(TaskHelpers::ipAddresses)
                .map(ipAddresses -> connector.deregisterAll(loadBalancerId, ipAddresses))
                .orElse(Completable.complete());

        return Completable.mergeDelayError(registerAll, deregisterAll)
                .andThen(Observable.just(batch))
                .doOnError(e -> logger.error("Error processing batch {}", batch, e))
                .onErrorResumeNext(Observable.empty());
    }

    private Observable<TargetStateBatchable> registerFromEvents(Observable<TaskUpdateEvent> events) {
        return events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isStartedWithIp)
                .flatMapIterable(task -> {
                    Set<JobLoadBalancer> loadBalancers = store.getAssociatedLoadBalancersSetForJob(task.getJobId());
                    if (!loadBalancers.isEmpty()) {
                        logger.info("Task update in job associated to one or more load balancers, registering {} load balancer targets: {}",
                                loadBalancers.size(), task.getJobId());
                    }
                    return updatesForLoadBalancers(loadBalancers, task, State.Registered);
                });
    }

    private Observable<TargetStateBatchable> deregisterFromEvents(Observable<TaskUpdateEvent> events) {
        return events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isTerminalWithIp)
                .flatMapIterable(task -> {
                    Set<JobLoadBalancer> loadBalancers = store.getAssociatedLoadBalancersSetForJob(task.getJobId());
                    if (!loadBalancers.isEmpty()) {
                        logger.info("Task update in job associated to one or more load balancers, deregistering {} load balancer targets: {}",
                                loadBalancers.size(), task.getJobId());
                    }
                    return updatesForLoadBalancers(loadBalancers, task, State.Deregistered);
                });
    }

    private Observable<TargetStateBatchable> moveFromEvents(Observable<TaskUpdateEvent> taskMovedEvents) {
        return taskMovedEvents.flatMapIterable(taskMoved -> {
            ArrayList<TargetStateBatchable> changes = new ArrayList<>();
            Task task = taskMoved.getCurrentTask();
            String targetJobId = task.getJobId();
            Set<JobLoadBalancer> targetJobLoadBalancers = store.getAssociatedLoadBalancersSetForJob(targetJobId);
            String sourceJobId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB);
            if (StringExt.isEmpty(sourceJobId)) {
                invariants.inconsistent("Task moved to %s does not include the source job id: %s", task.getJobId(), task.getId());
                return Collections.emptyList();
            }
            Set<JobLoadBalancer> sourceJobLoadBalancers = store.getAssociatedLoadBalancersSetForJob(sourceJobId);

            // operations below will dedup load balancers present in both source and target Sets, so we avoid
            // unnecessary churn by deregistering and registering the same targets on the same loadBalancerId

            // register on load balancers associated with the target job
            if (TaskHelpers.isStartedWithIp(task)) {
                Collection<JobLoadBalancer> toRegister = CollectionsExt.difference(
                        targetJobLoadBalancers, sourceJobLoadBalancers, JobLoadBalancer::byLoadBalancerId
                );
                changes.addAll(updatesForLoadBalancers(toRegister, task, State.Registered));
            }
            // deregister from load balancers associated with the source job
            Collection<JobLoadBalancer> toDeregister = CollectionsExt.difference(
                    sourceJobLoadBalancers, targetJobLoadBalancers, JobLoadBalancer::byLoadBalancerId
            );
            changes.addAll(updatesForLoadBalancers(toDeregister, task, State.Deregistered));

            if (!changes.isEmpty()) {
                logger.info("Task moved to {} from {}. Jobs are associated with one or more load balancers, generating {} load balancer updates",
                        targetJobId, sourceJobId, changes.size());
            }
            return changes;
        });
    }

    private List<TargetStateBatchable> updatesForLoadBalancers(Collection<JobLoadBalancer> loadBalancers, Task task, State desired) {
        return loadBalancers.stream()
                .map(association -> toLoadBalancerTarget(association, task))
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, desired)))
                .collect(Collectors.toList());
    }

    private Observable.Transformer<JobLoadBalancer, TargetStateBatchable> targetsForJobLoadBalancers(State state) {
        return jobLoadBalancers -> jobLoadBalancers
                .filter(jobLoadBalancer -> jobOperations.getJob(jobLoadBalancer.getJobId()).isPresent())
                .flatMap(jobLoadBalancer -> Observable.from(jobOperations.targetsForJob(jobLoadBalancer))
                        .doOnError(e -> logger.error("Error loading targets for jobId {}", jobLoadBalancer.getJobId(), e))
                        .onErrorResumeNext(Observable.empty()))
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, state)))
                .doOnError(e -> logger.error("Error fetching targets to {}", state, e))
                .retry();
    }

    private static LoadBalancerTarget toLoadBalancerTarget(JobLoadBalancer association, Task task) {
        return new LoadBalancerTarget(association, task.getId(),
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP));
    }

    private RateLimitedBatcher<TargetStateBatchable, String> buildBatcher() {
        final long minTimeMs = configuration.getMinTimeMs();
        final long maxTimeMs = configuration.getMaxTimeMs();
        final long bucketSizeMs = configuration.getBucketSizeMs();
        final LargestPerTimeBucket emissionStrategy = new LargestPerTimeBucket(minTimeMs, bucketSizeMs, scheduler);
        return RateLimitedBatcher.create(connectorTokenBucket, minTimeMs, maxTimeMs, TargetStateBatchable::getLoadBalancerId,
                emissionStrategy, METRIC_BATCHER, titusRuntime.getRegistry(), scheduler);
    }

    private Instant now() {
        return Instant.ofEpochMilli(scheduler.now());
    }
}
