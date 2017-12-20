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
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import io.netflix.titus.common.util.rx.batch.Batch;
import io.netflix.titus.common.util.rx.batch.LargestPerTimeBucket;
import io.netflix.titus.common.util.rx.batch.Priority;
import io.netflix.titus.common.util.rx.batch.RateLimitedBatcher;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

class LoadBalancerEngine {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerEngine.class);

    // TODO: index associations by jobId in the store, and remove this additional helper class
    private final AssociationsTracking associationsTracking = new AssociationsTracking();

    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

    private final LoadBalancerConfiguration configuration;
    private final JobOperations jobOperations;
    private final TokenBucket connectorTokenBucket;
    private final LoadBalancerConnector connector;
    private final LoadBalancerReconciler reconciler;
    private final Scheduler scheduler;

    LoadBalancerEngine(LoadBalancerConfiguration configuration, JobOperations jobOperations,
                       LoadBalancerStore loadBalancerStore, LoadBalancerReconciler reconciler,
                       LoadBalancerConnector loadBalancerConnector, TokenBucket connectorTokenBucket,
                       Scheduler scheduler) {
        // TODO(fabio): load tracking state from store
        this.configuration = configuration;
        this.jobOperations = jobOperations;
        this.connector = loadBalancerConnector;
        this.connectorTokenBucket = connectorTokenBucket;
        this.reconciler = reconciler;
        this.scheduler = scheduler;
    }

    public Completable add(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> {
            associationsTracking.add(jobLoadBalancer);
            pendingAssociations.onNext(jobLoadBalancer);
        });
    }

    public Completable remove(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> {
            associationsTracking.remove(jobLoadBalancer);
            pendingDissociations.onNext(jobLoadBalancer);
        });
    }

    Observable<Batch<TargetStateBatchable, String>> events() {
        Observable<TaskUpdateEvent> stateTransitions = jobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class)
                .filter(TaskHelpers::isStateTransition);

        final Observable<TargetStateBatchable> updates = Observable.merge(
                pendingAssociations.compose(targetsForJobLoadBalancers(State.Registered)),
                pendingDissociations.compose(targetsForJobLoadBalancers(State.Deregistered)),
                registerFromEvents(stateTransitions),
                deregisterFromEvents(stateTransitions),
                reconciler.events()
        ).compose(disableReconciliationTemporarily());

        return updates.lift(buildBatcher())
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
                reconciler.ignoreEventsFor(update.getIdentifier(), configuration.getReconciliation().getQuietPeriodMs(), TimeUnit.MILLISECONDS)
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

        final Completable registerAll = CollectionsExt.optional(byState.get(State.Registered))
                .map(TaskHelpers::ipAddresses)
                .map(ipAddresses -> connector.registerAll(loadBalancerId, ipAddresses))
                .orElse(Completable.complete());

        final Completable deregisterAll = CollectionsExt.optional(byState.get(State.Deregistered))
                .map(TaskHelpers::ipAddresses)
                .map(ipAddresses -> connector.deregisterAll(loadBalancerId, ipAddresses))
                .orElse(Completable.complete());

        return Completable.mergeDelayError(registerAll, deregisterAll)
                .andThen(Observable.just(batch))
                .doOnError(e -> logger.error("Error processing batch " + batch, e))
                .onErrorResumeNext(Observable.empty());
    }

    private Observable<TargetStateBatchable> registerFromEvents(Observable<TaskUpdateEvent> events) {
        // Optional.empty() tasks have been already filtered out
        //noinspection ConstantConditions
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isStartedWithIp);
        return targetsForTrackedTasks(tasks)
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Registered)));
    }

    private Observable<TargetStateBatchable> deregisterFromEvents(Observable<TaskUpdateEvent> events) {
        // Optional.empty() tasks have been already filtered out
        //noinspection ConstantConditions
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(TaskHelpers::isTerminalWithIp);
        return targetsForTrackedTasks(tasks)
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Deregistered)));
    }

    private Observable<LoadBalancerTarget> targetsForTrackedTasks(Observable<Task> tasks) {
        return tasks.doOnNext(task -> logger.debug("Checking if task is in job being tracked: {}", task))
                .filter(this::isTracked)
                .map(task -> Pair.of(task, associationsTracking.get(task.getJobId())))
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
        final long minTimeMs = configuration.getBatch().getMinTimeMs();
        final long maxTimeMs = configuration.getBatch().getMaxTimeMs();
        final long bucketSizeMs = configuration.getBatch().getBucketSizeMs();
        final LargestPerTimeBucket emissionStrategy = new LargestPerTimeBucket(minTimeMs, bucketSizeMs, scheduler);
        return RateLimitedBatcher.create(scheduler,
                connectorTokenBucket, minTimeMs, maxTimeMs, TargetStateBatchable::getLoadBalancerId, emissionStrategy);
    }

    private boolean isTracked(Task task) {
        return !associationsTracking.get(task.getJobId()).isEmpty();
    }

    private Instant now() {
        return Instant.ofEpochMilli(scheduler.now());
    }
}
