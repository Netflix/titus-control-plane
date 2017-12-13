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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
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

    private final AssociationsTracking associationsTracking = new AssociationsTracking();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

    private final LoadBalancerConfiguration configuration;
    private final V3JobOperations v3JobOperations;
    private final TargetTracking targetTracking;
    private final TokenBucket connectorTokenBucket;
    private final Scheduler scheduler;
    private final LoadBalancerConnector connector;

    LoadBalancerEngine(LoadBalancerConfiguration configuration, V3JobOperations v3JobOperations,
                       LoadBalancerStore loadBalancerStore, TargetTracking targetTracking,
                       LoadBalancerConnector loadBalancerConnector, TokenBucket connectorTokenBucket,
                       Scheduler scheduler) {
        // TODO(fabio): load tracking state from store
        this.configuration = configuration;
        this.v3JobOperations = v3JobOperations;
        this.targetTracking = targetTracking;
        this.connector = loadBalancerConnector;
        this.connectorTokenBucket = connectorTokenBucket;
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
        Observable<TaskUpdateEvent> stateTransitions = v3JobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class)
                .filter(TaskHelpers::isStateTransition);

        // TODO(fabio): rate limit changes from task events and API calls to allow space for reconciliation?
        final Observable<TargetStateBatchable> updates = Observable.merge(
                registerFromAssociations(pendingAssociations),
                deregisterFromDissociations(pendingDissociations),
                registerFromEvents(stateTransitions),
                deregisterFromEvents(stateTransitions)
        );

        return updates
                .lift(buildBatcher())
                .filter(batch -> !batch.getItems().isEmpty())
                .onBackpressureDrop(batch -> logger.warn("Backpressure! Dropping batch for {} size {}", batch.getIndex(), batch.size()))
                .doOnNext(batch -> logger.debug("Processing batch for {} size {}", batch.getIndex(), batch.size()))
                .flatMap(this::applyUpdates)
                .doOnNext(batch -> logger.info("Processed {} load balancer updates for {}", batch.size(), batch.getIndex()))
                .doOnError(e -> logger.error("Error batching load balancer calls", e))
                .retry();
    }

    public void shutdown() {
        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }

    private Observable<Batch<TargetStateBatchable, String>> applyUpdates(Batch<TargetStateBatchable, String> batch) {
        final String loadBalancerId = batch.getIndex();
        final Map<State, List<TargetStateBatchable>> byState = batch.getItems().stream()
                .collect(Collectors.groupingBy(TargetStateBatchable::getState));

        final Completable merged = Completable.mergeDelayError(
                trackAndRegister(loadBalancerId, byState),
                untrackAndDeregister(loadBalancerId, byState)
        );
        return merged.andThen(Observable.just(batch))
                .doOnError(e -> logger.error("Error processing batch " + batch, e))
                .onErrorResumeNext(Observable.empty());
    }

    private Completable trackAndRegister(String loadBalancerId, Map<State, List<TargetStateBatchable>> byState) {
        List<TargetStateBatchable> toRegister = byState.get(State.Registered);
        if (CollectionsExt.isNullOrEmpty(toRegister)) {
            return Completable.complete();
        }
        return targetTracking.updateTargets(toRegister)
                .andThen(connector.registerAll(loadBalancerId, ipAddresses(toRegister)));
    }

    private Completable untrackAndDeregister(String loadBalancerId, Map<State, List<TargetStateBatchable>> byState) {
        List<TargetStateBatchable> toDeregister = byState.get(State.Deregistered);
        if (CollectionsExt.isNullOrEmpty(toDeregister)) {
            return Completable.complete();
        }
        return targetTracking.removeTargets(targets(toDeregister))
                .andThen(connector.deregisterAll(loadBalancerId, ipAddresses(toDeregister)));
    }

    private Collection<LoadBalancerTarget> targets(Collection<TargetStateBatchable> from) {
        return from.stream().map(TargetStateBatchable::getIdentifier).collect(Collectors.toList());
    }

    private Set<String> ipAddresses(List<TargetStateBatchable> from) {
        return from.stream().map(TargetStateBatchable::getIpAddress).collect(Collectors.toSet());
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

    private Observable<TargetStateBatchable> registerFromAssociations(Observable<JobLoadBalancer> pendingAssociations) {
        return pendingAssociations
                .filter(jobLoadBalancer -> v3JobOperations.getJob(jobLoadBalancer.getJobId()).isPresent())
                .flatMap(jobLoadBalancer -> Observable.from(targetsForJob(jobLoadBalancer))
                        .doOnError(e -> logger.error("Error loading targets for jobId " + jobLoadBalancer.getJobId(), e))
                        .onErrorResumeNext(Observable.empty()))
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Registered)))
                .doOnError(e -> logger.error("Error fetching targets to register", e))
                .retry();
    }

    private Observable<TargetStateBatchable> deregisterFromDissociations(Observable<JobLoadBalancer> pendingDissociations) {
        return pendingDissociations
                .flatMap(
                        // fetch everything, including deregistered, so they are retried
                        jobLoadBalancer -> targetTracking.retrieveTargets(jobLoadBalancer)
                                .map(targetState -> new LoadBalancerTarget(
                                        jobLoadBalancer, targetState.getLoadBalancerTarget().getTaskId(), targetState.getLoadBalancerTarget().getIpAddress()
                                )))
                .map(target -> new TargetStateBatchable(Priority.High, now(), new TargetState(target, State.Deregistered)))
                .doOnError(e -> logger.error("Error fetching targets to deregister", e))
                .retry();
    }

    /**
     * Valid targets are tasks in the Started state that have ip addresses associated to them.
     */
    private List<LoadBalancerTarget> targetsForJob(JobLoadBalancer jobLoadBalancer) {
        return v3JobOperations.getTasks(jobLoadBalancer.getJobId()).stream()
                .filter(TaskHelpers::isStartedWithIp)
                .map(task -> new LoadBalancerTarget(
                        jobLoadBalancer,
                        task.getId(),
                        task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                ))
                .collect(Collectors.toList());
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
