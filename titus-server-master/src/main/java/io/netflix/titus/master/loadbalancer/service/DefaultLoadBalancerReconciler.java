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

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import io.netflix.titus.api.loadbalancer.model.TargetState;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.batch.Priority;
import io.netflix.titus.common.util.spectator.ContinuousSubscriptionMetrics;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

import static io.netflix.titus.api.jobmanager.service.JobManagerException.ErrorCode.JobNotFound;
import static io.netflix.titus.master.MetricConstants.METRIC_LOADBALANCER;

/**
 * This implementation assumes that it "owns" a LoadBalancer once jobs are associated with it, so no other systems can
 * be sharing the same LoadBalancer. All targets that are registered by external systems will be deregistered by this
 * reconciliation implementation.
 * <p>
 * This was a simple way to get a first version out of the door, but it will likely be changed in the future once we
 * have a good way to track which targets should be managed by this reconciler.
 */
public class DefaultLoadBalancerReconciler implements LoadBalancerReconciler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerReconciler.class);

    /**
     * how many store calls (both updates and deletes) are allowed concurrently during cleanup (GC)
     */
    private static final int MAX_ORPHAN_CLEANUP_CONCURRENCY = 100;

    private static final String METRIC_RECONCILER = METRIC_LOADBALANCER + "reconciliation";
    private static final String UNKNOWN_JOB = "UNKNOWN-JOB";
    private static final String UNKNOWN_TASK = "UNKNOWN-TASK";

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
    private final Registry registry;
    private final Scheduler scheduler;

    private final Counter registerCounter;
    private final Counter deregisterCounter;
    private final ContinuousSubscriptionMetrics fullReconciliationMetrics;
    private final ContinuousSubscriptionMetrics orphanUpdateMetrics;
    private final ContinuousSubscriptionMetrics removeMetrics;
    private final ContinuousSubscriptionMetrics registeredIpsMetrics;
    private final Id ignoredMetricsId;
    private final Id orphanMetricsId;

    DefaultLoadBalancerReconciler(LoadBalancerConfiguration configuration,
                                  LoadBalancerStore store,
                                  LoadBalancerConnector connector,
                                  LoadBalancerJobOperations loadBalancerJobOperations,
                                  Registry registry,
                                  Scheduler scheduler) {
        this.store = store;
        this.connector = connector;
        this.jobOperations = loadBalancerJobOperations;
        this.delayMs = configuration.getReconciliationDelayMs();
        this.timeoutMs = configuration::getReconciliationTimeoutMs;
        this.registry = registry;
        this.scheduler = scheduler;

        List<Tag> tags = Collections.singletonList(new BasicTag("class", DefaultLoadBalancerReconciler.class.getSimpleName()));
        final Id updatesCounterId = registry.createId(METRIC_RECONCILER + ".updates", tags);
        this.registerCounter = registry.counter(updatesCounterId.withTag("operation", "register"));
        this.deregisterCounter = registry.counter(updatesCounterId.withTag("operation", "deregister"));
        this.fullReconciliationMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".full", tags, registry);
        this.orphanUpdateMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".orphanUpdates", tags, registry);
        this.removeMetrics = SpectatorExt.continuousSubscriptionMetrics(METRIC_RECONCILER + ".remove", tags, registry);
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
        final Observable<Map.Entry<String, List<JobLoadBalancerState>>> cleanupOrphansAndSnapshot = updateOrphanAssociations()
                .andThen(snapshotAssociationsByLoadBalancer());

        // full reconciliation run
        final Observable<TargetStateBatchable> updatesForAll = cleanupOrphansAndSnapshot
                .flatMap(entry -> reconcile(entry.getKey(), entry.getValue()), 1)
                .compose(ObservableExt.subscriptionTimeout(timeoutMs, TimeUnit.MILLISECONDS, scheduler))
                .compose(fullReconciliationMetrics.asObservable())
                .doOnError(e -> logger.error("reconciliation failed", e))
                .onErrorResumeNext(Observable.empty());

        // schedule periodic full reconciliations
        return ObservableExt.periodicGenerator(updatesForAll, delayMs, delayMs, TimeUnit.MILLISECONDS, scheduler)
                .compose(SpectatorExt.subscriptionMetrics(METRIC_RECONCILER, DefaultLoadBalancerReconciler.class, registry))
                .flatMap(Observable::from, 1);
    }

    private Observable<TargetStateBatchable> reconcile(String loadBalancerId, List<JobLoadBalancerState> associations) {
        final Observable<TargetStateBatchable> updatesForLoadBalancer = connector.getRegisteredIps(loadBalancerId)
                // the same metrics transformer can be used for all subscriptions only because they are all being
                // serialized with flatMap(maxConcurrent: 1)
                .compose(registeredIpsMetrics.asSingle())
                .flatMapObservable(registeredIps -> updatesFor(loadBalancerId, associations, registeredIps));

        return updatesForLoadBalancer
                .doOnError(e -> logger.error("Error while reconciling load balancer {}", loadBalancerId, e))
                .onErrorResumeNext(Observable.empty());
    }

    /**
     * Generates a stream of necessary updates based on what jobs are currently associated with a load balancer, and the
     * ip addresses currently registered on it.
     * <p>
     * {@link JobLoadBalancer} associations in the <tt>Dissociated</tt> state will be removed when it is safe to do so,
     * i.e.: when there is nothing from them to be deregistered on the load balancer anymore.
     *
     * @param associations           jobs currently associated to the load balancer
     * @param ipsCurrentlyRegistered ip addresses currently registered on the load balancer
     */
    private Observable<TargetStateBatchable> updatesFor(String loadBalancerId, List<JobLoadBalancerState> associations, Set<String> ipsCurrentlyRegistered) {
        final Instant now = now();
        final Set<LoadBalancerTarget> shouldBeRegistered = associations.stream()
                .filter(JobLoadBalancerState::isStateAssociated)
                .flatMap(association -> targetsForJobSafe(association).stream())
                .collect(Collectors.toSet());
        final Set<String> shouldBeRegisteredIps = shouldBeRegistered.stream()
                .map(LoadBalancerTarget::getIpAddress)
                .collect(Collectors.toSet());

        Set<LoadBalancerTarget> toRegister = shouldBeRegistered.stream()
                .filter(target -> !ipsCurrentlyRegistered.contains(target.getIpAddress()))
                .collect(Collectors.toSet());
        Set<LoadBalancerTarget> toDeregister = CollectionsExt.copyAndRemove(ipsCurrentlyRegistered, shouldBeRegisteredIps).stream()
                .map(ip -> updateForUnknownTask(loadBalancerId, ip))
                .collect(Collectors.toSet());
        reportUpdates(toRegister, toDeregister);

        final Observable<TargetStateBatchable> updatesForLoadBalancer = Observable.from(CollectionsExt.merge(
                withState(now, toRegister, State.Registered),
                withState(now, toDeregister, State.Deregistered)
        )).filter(this::isNotIgnored);

        // clean up Dissociated entries when all their targets have been deregistered
        final Completable removeOperations = toDeregister.isEmpty() ?
                Completable.mergeDelayError(removeAllDissociated(associations), MAX_ORPHAN_CLEANUP_CONCURRENCY)
                        .doOnSubscribe(ignored -> logger.debug("Cleaning up dissociated jobs for load balancer {}", loadBalancerId))
                        .compose(removeMetrics.asCompletable())
                        .doOnError(e -> logger.error("Error while cleaning up associations", e))
                        .onErrorComplete()
                : Completable.complete() /* don't remove anything when there still are targets to be deregistered */;

        return removeOperations.andThen(updatesForLoadBalancer);
    }

    private void reportUpdates(Set<LoadBalancerTarget> toRegister, Set<LoadBalancerTarget> toDeregister) {
        boolean found = false;
        if (!toRegister.isEmpty()) {
            found = true;
            registerCounter.increment(toRegister.size());
        }
        if (!toDeregister.isEmpty()) {
            found = true;
            deregisterCounter.increment(toDeregister.size());
        }
        if (found) {
            logger.info("Reconciliation found targets to to be registered: {}, to be deregistered: {}",
                    toRegister.size(), toDeregister.size());
        }
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
                                .doOnSubscribe(ignored -> logger.debug("Removing dissociated {}", association))
                                .doOnError(e -> logger.error("Failed to remove {}", association, e))
                                .onErrorComplete()
                ).collect(Collectors.toList());
        return Observable.from(removeOperations);
    }

    /**
     * Hack until we start keeping track of everything that was registered on a load balancer. For now, we generate
     * <tt>State.Deregistered</tt> updates for dummy tasks since we only care about the <tt>loadBalancerId</tt> and
     * the <tt>ipAddress</tt> for deregistrations.
     */
    private LoadBalancerTarget updateForUnknownTask(String loadBalancerId, String ip) {
        return new LoadBalancerTarget(new JobLoadBalancer(UNKNOWN_JOB, loadBalancerId), UNKNOWN_TASK, ip);
    }

    private List<TargetStateBatchable> withState(Instant instant, Collection<LoadBalancerTarget> targets, State state) {
        return targets.stream()
                .map(target -> new TargetStateBatchable(Priority.Low, instant, new TargetState(target, state)))
                .collect(Collectors.toList());
    }

    /**
     * @return emit loadBalancerId -> listOfAssociation pairs to subscribers
     */
    private Observable<Map.Entry<String, List<JobLoadBalancerState>>> snapshotAssociationsByLoadBalancer() {
        return Observable.defer(() -> {
            cleanupExpiredIgnored();
            logger.debug("Snapshotting current associations");
            final Set<Map.Entry<String, List<JobLoadBalancerState>>> pairs = store.getAssociations().stream()
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
        final Observable<Completable> updateOperations = Observable.from(markedAsOrphan).map(marked -> {
            if (jobOperations.getJob(marked.getJobId()).isPresent()) {
                logger.warn("Not updating an association that was previously marked as orphan, but now contains an existing job: {}", marked);
                return Completable.complete();
            }
            return store.addOrUpdateLoadBalancer(marked, JobLoadBalancer.State.Dissociated)
                    .doOnSubscribe(ignored -> logger.info("Setting orphan association as Dissociated: {}", marked))
                    .doOnError(e -> logger.error("Failed to remove {}", marked, e));
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
}
