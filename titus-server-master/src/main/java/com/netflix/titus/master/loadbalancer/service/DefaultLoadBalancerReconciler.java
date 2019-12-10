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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.connector.cloud.LoadBalancer;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.batch.Priority;
import com.netflix.titus.common.util.spectator.ContinuousSubscriptionMetrics;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.netflix.titus.api.jobmanager.service.JobManagerException.ErrorCode.JobNotFound;
import static com.netflix.titus.master.MetricConstants.METRIC_LOADBALANCER;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.partitioningBy;

public class DefaultLoadBalancerReconciler implements LoadBalancerReconciler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerReconciler.class);

    private static final String METRIC_RECONCILER = METRIC_LOADBALANCER + "reconciliation";
    private static final Runnable NOOP = () -> {
    };
    /**
     * how many store calls (both updates and deletes) are allowed concurrently during cleanup (GC)
     */
    private static final int MAX_ORPHAN_CLEANUP_CONCURRENCY = 100;

    private final ConcurrentMap<LoadBalancerTarget, Instant> ignored = new ConcurrentHashMap<>();

    // this is not being accessed by multiple threads at the same time, but we still use a ConcurrentMap to ensure
    // visibility across multiple reconciliation runs, which may run on different threads
    private final Set<JobLoadBalancer> markedAsOrphan = ConcurrentHashMap.newKeySet();

    private final LoadBalancerStore store;
    private final LoadBalancerConnector connector;
    private final LoadBalancerJobOperations jobOperations;
    // TODO: make dynamic and switch to a Supplier<Long>
    private final long delayMs;
    private final Supplier<Long> timeoutMs;
    private final Runnable afterReconciliation;
    private final Registry registry;
    private final Scheduler scheduler;

    private final Counter registerCounter;
    private final Counter deregisterCounter;
    private final Counter removeCounter;
    private final ContinuousSubscriptionMetrics fullReconciliationMetrics;
    private final ContinuousSubscriptionMetrics orphanUpdateMetrics;
    private final ContinuousSubscriptionMetrics removeMetrics;
    private final ContinuousSubscriptionMetrics removeTargetsMetrics;
    private final ContinuousSubscriptionMetrics registeredIpsMetrics;
    private final Id ignoredMetricsId;
    private final Id orphanMetricsId;

    DefaultLoadBalancerReconciler(LoadBalancerConfiguration configuration,
                                  LoadBalancerStore store,
                                  LoadBalancerConnector connector,
                                  LoadBalancerJobOperations loadBalancerJobOperations,
                                  Registry registry,
                                  Scheduler scheduler) {
        this(configuration, store, connector, loadBalancerJobOperations, NOOP, registry, scheduler);

    }

    DefaultLoadBalancerReconciler(LoadBalancerConfiguration configuration,
                                  LoadBalancerStore store,
                                  LoadBalancerConnector connector,
                                  LoadBalancerJobOperations loadBalancerJobOperations,
                                  Runnable afterReconciliation,
                                  Registry registry,
                                  Scheduler scheduler) {
        this.store = store;
        this.connector = connector;
        this.jobOperations = loadBalancerJobOperations;
        this.delayMs = configuration.getReconciliationDelayMs();
        this.timeoutMs = configuration::getReconciliationTimeoutMs;
        this.afterReconciliation = afterReconciliation;
        this.registry = registry;
        this.scheduler = scheduler;

        List<Tag> tags = Collections.singletonList(new BasicTag("class", DefaultLoadBalancerReconciler.class.getSimpleName()));
        final Id updatesCounterId = registry.createId(METRIC_RECONCILER + ".updates", tags);
        this.registerCounter = registry.counter(updatesCounterId.withTag("operation", "register"));
        this.deregisterCounter = registry.counter(updatesCounterId.withTag("operation", "deregister"));
        this.removeCounter = registry.counter(updatesCounterId.withTag("operation", "remove"));
        this.fullReconciliationMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".full", tags, registry);
        this.orphanUpdateMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".orphanUpdates", tags, registry);
        this.removeMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".remove", tags, registry);
        this.removeTargetsMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".removeTargets", tags, registry);
        this.registeredIpsMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".getRegisteredIps", tags, registry);
        this.ignoredMetricsId = registry.createId(METRIC_RECONCILER + ".ignored", tags);
        this.orphanMetricsId = registry.createId(METRIC_RECONCILER + ".orphan", tags);
        PolledMeter.using(registry).withId(ignoredMetricsId).monitorSize(ignored);
        PolledMeter.using(registry).withId(orphanMetricsId).monitorSize(markedAsOrphan);
    }

    @Override
    public void activateCooldownFor(LoadBalancerTarget target, long period, TimeUnit unit) {
        Duration periodDuration = Duration.ofMillis(unit.toMillis(period));
        logger.debug("Setting a cooldown of {} for target {}", periodDuration, target);
        Instant untilWhen = Instant.ofEpochMilli(scheduler.now()).plus(periodDuration);
        ignored.put(target, untilWhen);
    }

    @Override
    public void shutdown() {
        orphanUpdateMetrics.remove();
        removeMetrics.remove();
        registeredIpsMetrics.remove();
        PolledMeter.remove(registry, ignoredMetricsId);
        PolledMeter.remove(registry, orphanMetricsId);
    }

    @Override
    public Observable<TargetStateBatchable> events() {
        Observable<Map.Entry<String, List<JobLoadBalancerState>>> cleanupOrphansAndSnapshot = updateOrphanAssociations()
                .andThen(snapshotAssociationsByLoadBalancer());

        // full reconciliation run
        Observable<TargetStateBatchable> updatesForAll = cleanupOrphansAndSnapshot
                .flatMap(entry -> reconcile(entry.getKey(), entry.getValue()), 1)
                .compose(ObservableExt.subscriptionTimeout(timeoutMs, TimeUnit.MILLISECONDS, scheduler))
                .compose(fullReconciliationMetrics.asObservable())
                .doOnError(e -> logger.error("reconciliation failed", e))
                .onErrorResumeNext(Observable.empty());

        // schedule periodic full reconciliations
        return ObservableExt.periodicGenerator(updatesForAll, delayMs, delayMs, TimeUnit.MILLISECONDS, scheduler)
                .compose(SpectatorExt.subscriptionMetrics(METRIC_RECONCILER, DefaultLoadBalancerReconciler.class, registry))
                .flatMap(iterable -> Observable.from(iterable)
                        .doOnTerminate(afterReconciliation::run), 1);
    }

    private Observable<TargetStateBatchable> reconcile(String loadBalancerId, List<JobLoadBalancerState> associations) {
        Observable<TargetStateBatchable> updatesForLoadBalancer = connector.getLoadBalancer(loadBalancerId)
                // merge known targets
                .flatMap(loadBalancer -> ReactorExt.toSingle(
                        store.getLoadBalancerTargets(loadBalancer.getId())
                                .collect(Collectors.toSet())
                                .map(knownTargets -> new LoadBalancerWithKnownTargets(loadBalancer, knownTargets))
                ))
                // the same metrics transformer can be used for all subscriptions only because they are all being
                // serialized with flatMap(maxConcurrent: 1)
                .compose(registeredIpsMetrics.asSingle())
                .flatMapObservable(loadBalancerTargets -> updatesFor(loadBalancerTargets, associations));

        return updatesForLoadBalancer
                .doOnError(e -> logger.error("Error while reconciling load balancer {}", loadBalancerId, e))
                .onErrorResumeNext(Observable.empty());
    }

    /**
     * Generates a stream of necessary updates based on what jobs are currently associated with a load balancer, the
     * ip addresses currently registered on it, and what ips were previously registered by us.
     * <p>
     * {@link JobLoadBalancer} associations in the <tt>Dissociated</tt> state will be removed when it is safe to do so,
     * i.e.: when there is no more stored state to be cleaned up, and nothing to be deregistered on the load balancer.
     *
     * @param loadBalancer tuple with ip addresses currently registered on the load balancer, and ip addresses
     *                     previously registered by us
     * @param associations jobs currently associated to the load balancer
     */
    private Observable<TargetStateBatchable> updatesFor(LoadBalancerWithKnownTargets loadBalancer,
                                                        List<JobLoadBalancerState> associations) {
        Instant now = now();

        ReconciliationUpdates updates = (loadBalancer.current.getState().equals(LoadBalancer.State.ACTIVE)) ?
                updatesForActiveLoadBalancer(loadBalancer, associations)
                : updatesForRemovedLoadBalancer(loadBalancer, associations);

        Completable cleanupTargets = (!updates.toRemove.isEmpty()) ?
                ReactorExt.toCompletable(store.removeDeregisteredTargets(updates.toRemove))
                        // bring processing back the the Rx threads, otherwise it happens in the C* driver threadpool
                        .observeOn(Schedulers.computation())
                        .doOnSubscribe(ignored -> logger.info("Cleaning up {} deregistered targets for load balancer {}",
                                updates.toRemove.size(), loadBalancer.current.getId()))
                        .compose(removeTargetsMetrics.asCompletable())
                        .doOnError(e -> logger.error("Error while cleaning up targets for " + loadBalancer.current.getId(), e))
                        .onErrorComplete()
                : Completable.complete();

        // clean up dissociated entries only it is safe: all targets have been deregistered and there is nothing to be removed
        Completable cleanupDissociated = (updates.toDeregister.isEmpty() && updates.toRemove.isEmpty()) ?
                Completable.mergeDelayError(removeAllDissociated(associations), MAX_ORPHAN_CLEANUP_CONCURRENCY)
                        .doOnSubscribe(ignored -> logger.debug("Cleaning up dissociated jobs for load balancer {}", loadBalancer.current.getId()))
                        .compose(removeMetrics.asCompletable())
                        .doOnError(e -> logger.error("Error while cleaning up associations for " + loadBalancer.current.getId(), e))
                        .onErrorComplete()
                : Completable.complete();

        Observable<TargetStateBatchable> updatesForLoadBalancer = Observable.from(CollectionsExt.merge(
                withState(now, updates.toRegister, State.REGISTERED),
                withState(now, updates.toDeregister, State.DEREGISTERED)
        )).filter(this::isNotIgnored);

        return Completable.merge(cleanupTargets, cleanupDissociated)
                .andThen(updatesForLoadBalancer);
    }

    private ReconciliationUpdates updatesForActiveLoadBalancer(LoadBalancerWithKnownTargets loadBalancer, List<JobLoadBalancerState> associations) {
        Set<LoadBalancerTarget> shouldBeRegistered = associations.stream()
                .filter(JobLoadBalancerState::isStateAssociated)
                .flatMap(association -> targetsForJobSafe(association).stream())
                .collect(Collectors.toSet());

        Set<LoadBalancerTarget> toRegister = shouldBeRegistered.stream()
                .filter(target -> !loadBalancer.current.getRegisteredIps().contains(target.getIpAddress()))
                .collect(Collectors.toSet());

        // we will force deregistration of inconsistent entries to force their state to be updated
        Set<LoadBalancerTarget> inconsistentStoredState = loadBalancer.knownTargets.stream()
                .filter(target -> target.getState().equals(State.REGISTERED) &&
                        !shouldBeRegistered.contains(target.getLoadBalancerTarget())
                )
                .map(LoadBalancerTargetState::getLoadBalancerTarget)
                .collect(Collectors.toSet());

        Set<LoadBalancerTarget> shouldBeDeregisteredFromStoredState = loadBalancer.knownTargets.stream()
                .filter(target -> target.getState().equals(State.DEREGISTERED) &&
                        // filter out cases where job state hasn't fully propagated yet
                        !shouldBeRegistered.contains(target.getLoadBalancerTarget())
                )
                .map(LoadBalancerTargetState::getLoadBalancerTarget)
                .collect(Collectors.toSet());


        // Split what should be deregistered in:
        // 1. what needs to be deregistered (and is still registered in the loadbalancer)
        // 2. what state can be GCed because it has been already deregistered
        Map<Boolean, Set<LoadBalancerTarget>> toDeregisterCurrentInLoadBalancer = shouldBeDeregisteredFromStoredState.stream()
                .collect(partitioningBy(
                        target -> loadBalancer.current.getRegisteredIps().contains(target.getIpAddress()),
                        Collectors.toSet()
                ));
        Set<LoadBalancerTarget> toDeregister = Sets.union(toDeregisterCurrentInLoadBalancer.get(true), inconsistentStoredState);
        Set<LoadBalancerTarget> toRemove = toDeregisterCurrentInLoadBalancer.get(false);
        return new ReconciliationUpdates(loadBalancer.current.getId(), toRegister, toDeregister, toRemove);
    }

    private ReconciliationUpdates updatesForRemovedLoadBalancer(LoadBalancerWithKnownTargets loadBalancer, List<JobLoadBalancerState> associations) {
        logger.warn("Load balancer is gone, ignoring its associations (marking them to be GCed later), and removing known state for its targets: {}",
                loadBalancer.current.getId());
        for (JobLoadBalancerState association : associations) {
            if (association.isStateAssociated()) {
                logger.info("Marking association as orphan: {}", association.getJobLoadBalancer());
                markedAsOrphan.add(association.getJobLoadBalancer());
            }
        }
        // we will force deregistration of everything that was marked as REGISTERED to force its state to be updated
        Map<Boolean, Set<LoadBalancerTarget>> registeredOrNot = loadBalancer.knownTargets.stream()
                .collect(partitioningBy(
                        target -> target.getState().equals(State.REGISTERED),
                        mapping(LoadBalancerTargetState::getLoadBalancerTarget, Collectors.toSet())
                ));
        Set<LoadBalancerTarget> toDeregister = registeredOrNot.get(true); // all REGISTERED
        Set<LoadBalancerTarget> toRemove = registeredOrNot.get(false); // all DEREGISTERED
        return new ReconciliationUpdates(loadBalancer.current.getId(), Collections.emptySet(), toDeregister, toRemove);
    }

    private boolean isNotIgnored(TargetStateBatchable update) {
        return !ignored.containsKey(update.getIdentifier());
    }

    private List<LoadBalancerTarget> targetsForJobSafe(JobLoadBalancerState association) {
        try {
            return jobOperations.targetsForJob(association.getJobLoadBalancer());
        } catch (RuntimeException e) {
            if (JobManagerException.hasErrorCode(e, JobNotFound)) {
                logger.warn("Job is gone, ignoring its association and marking it to be GCed later {}", association);
                markedAsOrphan.add(association.getJobLoadBalancer());
            } else {
                logger.error("Ignoring association, unable to fetch targets for {}", association, e);
            }
            return Collections.emptyList();
        }
    }

    private Observable<Completable> removeAllDissociated(List<JobLoadBalancerState> associations) {
        final List<Completable> removeOperations = associations.stream()
                .filter(JobLoadBalancerState::isStateDissociated)
                .map(JobLoadBalancerState::getJobLoadBalancer)
                .map(association ->
                        store.removeLoadBalancer(association)
                                // bring processing back the the Rx threads, otherwise it happens in the C* driver threadpool
                                .observeOn(Schedulers.computation())
                                .doOnSubscribe(ignored -> logger.info("Removing dissociated {}", association))
                                .doOnError(e -> logger.error("Failed to remove {}", association, e))
                                .onErrorComplete()
                ).collect(Collectors.toList());
        return Observable.from(removeOperations);
    }

    private List<TargetStateBatchable> withState(Instant instant, Collection<LoadBalancerTarget> targets, State state) {
        return targets.stream()
                .map(target -> new TargetStateBatchable(Priority.LOW, instant, new LoadBalancerTargetState(target, state)))
                .collect(Collectors.toList());
    }

    /**
     * @return emit loadBalancerId -> listOfAssociation pairs to subscribers
     */
    private Observable<Map.Entry<String, List<JobLoadBalancerState>>> snapshotAssociationsByLoadBalancer() {
        return Observable.defer(() -> {
            cleanupExpiredIgnored();
            logger.debug("Snapshotting current associations");
            Set<Map.Entry<String, List<JobLoadBalancerState>>> pairs = store.getAssociations().stream()
                    .collect(Collectors.groupingBy(JobLoadBalancerState::getLoadBalancerId))
                    .entrySet();
            return Observable.from(pairs);
        });
    }

    private void cleanupExpiredIgnored() {
        Instant now = Instant.ofEpochMilli(scheduler.now());
        ignored.forEach((target, untilWhen) -> {
            if (untilWhen.isAfter(now)) {
                return;
            }
            if (ignored.remove(target, untilWhen) /* do not remove when changed */) {
                logger.debug("Cooldown expired for target {}", target);
            }
        });
    }

    /**
     * set previously marked orphan associations (their jobs are gone) as <tt>Dissociated</tt>.
     */
    private Completable updateOrphanAssociations() {
        Observable<Completable> updateOperations = Observable.from(markedAsOrphan).map(marked -> {
            if (jobOperations.getJob(marked.getJobId()).isPresent()) {
                logger.warn("Not updating an association that was previously marked as orphan, but now contains an existing job: {}", marked);
                return Completable.complete();
            }
            return store.addOrUpdateLoadBalancer(marked, JobLoadBalancer.State.DISSOCIATED)
                    // bring processing back the the Rx threads, otherwise it happens in the C* driver threadpool
                    .observeOn(Schedulers.computation())
                    .doOnSubscribe(ignored -> logger.info("Setting orphan association as Dissociated: {}", marked))
                    .doOnError(e -> logger.error("Failed to update to Dissociated {}", marked, e));
        });

        // do as much as possible and swallow errors since future reconciliations will mark orphan associations again
        return Completable.mergeDelayError(updateOperations, MAX_ORPHAN_CLEANUP_CONCURRENCY)
                .doOnSubscribe(s -> logger.debug("Updating orphan associations"))
                .compose(orphanUpdateMetrics.asCompletable())
                .onErrorComplete()
                .doOnTerminate(markedAsOrphan::clear);
    }

    private Instant now() {
        return Instant.ofEpochMilli(scheduler.now());
    }

    private static class LoadBalancerWithKnownTargets {
        private final LoadBalancer current;
        /**
         * Targets that have been previously registered by us.
         */
        private final Set<LoadBalancerTargetState> knownTargets;

        private LoadBalancerWithKnownTargets(LoadBalancer current, Set<LoadBalancerTargetState> knownTargets) {
            this.current = current;
            this.knownTargets = knownTargets;
        }
    }

    private class ReconciliationUpdates {
        private final String loadBalancerId;
        private final Set<LoadBalancerTarget> toRegister;
        private final Set<LoadBalancerTarget> toDeregister;
        private final Set<LoadBalancerTarget> toRemove;

        private ReconciliationUpdates(String loadBalancerId, Set<LoadBalancerTarget> toRegister,
                                      Set<LoadBalancerTarget> toDeregister, Set<LoadBalancerTarget> toRemove) {
            this.loadBalancerId = loadBalancerId;
            this.toRegister = toRegister;
            this.toDeregister = toDeregister;
            this.toRemove = toRemove;

            report();
        }

        private void report() {
            boolean found = false;
            if (!toRegister.isEmpty()) {
                found = true;
                registerCounter.increment(toRegister.size());
            }
            if (!toDeregister.isEmpty()) {
                found = true;
                deregisterCounter.increment(toDeregister.size());
            }
            if (!toRemove.isEmpty()) {
                found = true;
                removeCounter.increment(toRemove.size());
            }
            if (found) {
                logger.info("Reconciliation for load balancer {} found targets to to be registered: {}, to be deregistered: {}, to be removed: {}",
                        loadBalancerId, toRegister.size(), toDeregister.size(), toRemove.size());
            }
        }

    }
}
