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

package io.netflix.titus.master.loadbalancer.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import io.netflix.titus.api.loadbalancer.model.TargetState;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.batch.Batch;
import io.netflix.titus.common.util.rx.batch.LargestPerTimeBucket;
import io.netflix.titus.common.util.rx.batch.Priority;
import io.netflix.titus.common.util.rx.batch.RateLimitedBatcher;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static io.netflix.titus.master.MetricConstants.METRIC_LOADBALANCER;

class LoadBalancerEngine {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerEngine.class);

    private static final String METRIC_BATCHES = METRIC_LOADBALANCER + "batches";
    private static final String METRIC_BATCHER = METRIC_LOADBALANCER + "batcher";

    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

    private final TitusRuntime titusRuntime;
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

        Observable<TaskUpdateEvent> stateTransitions = jobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class)
                .filter(TaskHelpers::isStateTransition);

        final Observable<TargetStateBatchable> updates = Observable.merge(
                reconcilerEvents,
                pendingAssociations.compose(targetsForJobLoadBalancers(State.Registered)),
                pendingDissociations.compose(targetsForJobLoadBalancers(State.Deregistered)),
                registerFromEvents(stateTransitions),
                deregisterFromEvents(stateTransitions)
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
                .doOnError(e -> logger.error("Error processing batch " + batch, e))
                .onErrorResumeNext(Observable.empty());
    }

    private Observable<TargetStateBatchable> registerFromEvents(Observable<TaskUpdateEvent> events) {
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isStartedWithIp);
        return targetsForTrackedTasks(tasks)
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Registered)));
    }

    private Observable<TargetStateBatchable> deregisterFromEvents(Observable<TaskUpdateEvent> events) {
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isTerminalWithIp);
        return targetsForTrackedTasks(tasks)
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Deregistered)));
    }

    private Observable<LoadBalancerTarget> targetsForTrackedTasks(Observable<Task> tasks) {
        return tasks.doOnNext(task -> logger.debug("Checking if task in job is being tracked: {}", task.getId()))
                .map(task -> Pair.of(task, store.getAssociatedLoadBalancersSetForJob(task.getJobId())))
                // A task with an empty set is not tracked by any load balancer
                .filter(pair -> !pair.getRight().isEmpty())
                .doOnNext(pair -> logger.info("Task update in job being tracked, enqueuing {} load balancer updates: {}",
                        pair.getRight().size(), pair.getLeft()))
                .flatMap(pair -> Observable.from(
                        pair.getRight().stream().map(association -> new LoadBalancerTarget(
                                association,
                                pair.getLeft().getId(),
                                pair.getLeft().getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                        )).collect(Collectors.toList()))
                );
    }

    private Observable.Transformer<JobLoadBalancer, TargetStateBatchable> targetsForJobLoadBalancers(State state) {
        return jobLoadBalancers -> jobLoadBalancers
                .filter(jobLoadBalancer -> jobOperations.getJob(jobLoadBalancer.getJobId()).isPresent())
                .flatMap(jobLoadBalancer -> Observable.from(jobOperations.targetsForJob(jobLoadBalancer))
                        .doOnError(e -> logger.error("Error loading targets for jobId " + jobLoadBalancer.getJobId(), e))
                        .onErrorResumeNext(Observable.empty()))
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, state)))
                .doOnError(e -> logger.error("Error fetching targets to " + state.toString(), e))
                .retry();
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
