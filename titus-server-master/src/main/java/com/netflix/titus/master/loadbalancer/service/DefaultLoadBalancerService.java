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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.LoadBalancer;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.common.util.rx.batch.Batch;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final TitusRuntime runtime;
    private final LoadBalancerConfiguration configuration;
    private final LoadBalancerConnector loadBalancerConnector;
    private final LoadBalancerStore loadBalancerStore;
    private final LoadBalancerJobValidator validator;
    private final LoadBalancerReconciler reconciler;
    private final LoadBalancerEngine engine;
    private final Scheduler scheduler;

    private Subscription loadBalancerBatches;

    @Inject
    public DefaultLoadBalancerService(TitusRuntime runtime,
                                      LoadBalancerConfiguration configuration,
                                      LoadBalancerConnector loadBalancerConnector,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations,
                                      LoadBalancerJobValidator validator) {
        this(runtime, configuration, loadBalancerConnector, loadBalancerStore,
                new LoadBalancerJobOperations(v3JobOperations),
                new DefaultLoadBalancerReconciler(
                        configuration, loadBalancerStore, loadBalancerConnector,
                        new LoadBalancerJobOperations(v3JobOperations), runtime.getRegistry(), Schedulers.computation()
                ), validator, Schedulers.computation()
        );
    }

    @VisibleForTesting
    DefaultLoadBalancerService(TitusRuntime runtime,
                               LoadBalancerConfiguration configuration,
                               LoadBalancerConnector loadBalancerConnector,
                               LoadBalancerStore loadBalancerStore,
                               LoadBalancerJobOperations loadBalancerJobOperations,
                               LoadBalancerReconciler reconciler,
                               LoadBalancerJobValidator validator,
                               Scheduler scheduler) {
        this.runtime = runtime;
        this.configuration = configuration;
        this.loadBalancerConnector = loadBalancerConnector;
        this.loadBalancerStore = loadBalancerStore;
        this.reconciler = reconciler;
        this.validator = validator;
        this.scheduler = scheduler;

        final long burst = configuration.getRateLimitBurst();
        final long refillPerSec = configuration.getRateLimitRefillPerSec();
        final TokenBucket connectorTokenBucket = Limiters.createFixedIntervalTokenBucket("loadBalancerConnector",
                burst, burst, refillPerSec, 1, TimeUnit.SECONDS);
        this.engine = new LoadBalancerEngine(runtime, configuration, loadBalancerJobOperations, reconciler,
                loadBalancerConnector, loadBalancerStore, connectorTokenBucket, scheduler);

    }

    @Override
    public Observable<String> getJobLoadBalancers(String jobId) {
        return loadBalancerStore.getAssociatedLoadBalancersForJob(jobId)
                .map(JobLoadBalancer::getLoadBalancerId);
    }

    @Override
    public Pair<List<JobLoadBalancer>, Pagination> getAllLoadBalancers(Page page) {
        if (StringExt.isNotEmpty(page.getCursor())) {
            final List<JobLoadBalancer> allLoadBalancers = loadBalancerStore.getAssociations().stream()
                    .map(JobLoadBalancerState::getJobLoadBalancer)
                    .sorted(LoadBalancerCursors.loadBalancerComparator())
                    .collect(Collectors.toList());

            return PaginationUtil.takePageWithCursor(Page.newBuilder().withPageSize(page.getPageSize()).withCursor(page.getCursor()).build(),
                    allLoadBalancers,
                    LoadBalancerCursors.loadBalancerComparator(),
                    LoadBalancerCursors::loadBalancerIndexOf,
                    LoadBalancerCursors::newCursorFrom);
        }

        // no cursor provided
        int offset = page.getPageSize() * page.getPageNumber();
        // Grab an extra item so we can tell if there's more to read after offset+limit.
        int limit = page.getPageSize() + 1;
        List<JobLoadBalancer> jobLoadBalancerPageList = loadBalancerStore.getAssociationsPage(offset, limit);

        boolean hasMore = jobLoadBalancerPageList.size() > page.getPageSize();
        jobLoadBalancerPageList = hasMore ? jobLoadBalancerPageList.subList(0, page.getPageSize()) : jobLoadBalancerPageList;
        final String cursor = jobLoadBalancerPageList.isEmpty() ? "" : LoadBalancerCursors.newCursorFrom(jobLoadBalancerPageList.get(jobLoadBalancerPageList.size() - 1));
        return Pair.of(jobLoadBalancerPageList, new Pagination(page, hasMore, 1, jobLoadBalancerPageList.size(), cursor, 0));
    }

    @Override
    public Completable addLoadBalancer(String jobId, String loadBalancerId) {
        try {
            validator.validateJobId(jobId);
        } catch (Exception e) {
            return Completable.error(TitusServiceException.invalidArgument(e.getMessage()));
        }

        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.ASSOCIATED)
                .doOnCompleted(() -> engine.add(jobLoadBalancer));
    }

    @Override
    public Completable removeLoadBalancer(String jobId, String loadBalancerId) {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.DISSOCIATED)
                .doOnCompleted(() -> engine.remove(jobLoadBalancer));
    }

    @Activator
    public void activate() {
        if (!configuration.isEngineEnabled()) {
            return; // noop
        }
        if (configuration.isTargetsToStoreBackfillEnabled()) {
            backfillTargetsToStore();
        }

        loadBalancerBatches = runtime.persistentStream(events())
                .subscribeOn(scheduler)
                .subscribe(
                        this::logBatchInfo,
                        e -> logger.error("Error while processing load balancer batch", e),
                        () -> logger.info("Load balancer batch stream closed")
                );
    }

    private void logBatchInfo(Batch<TargetStateBatchable, String> batch) {
        final String loadBalancerId = batch.getIndex();
        final Map<LoadBalancerTarget.State, List<TargetStateBatchable>> byState = batch.getItems().stream()
                .collect(Collectors.groupingBy(TargetStateBatchable::getState));
        final int registered = byState.getOrDefault(LoadBalancerTarget.State.REGISTERED, Collections.emptyList()).size();
        final int deregistered = byState.getOrDefault(LoadBalancerTarget.State.DEREGISTERED, Collections.emptyList()).size();
        logger.info("Load balancer {} batch completed. To be registered: {}, to be deregistered: {}", loadBalancerId, registered, deregistered);
    }

    @Deactivator
    public void deactivate() {
        ObservableExt.safeUnsubscribe(loadBalancerBatches);
        engine.shutdown();
        reconciler.shutdown();
    }

    @VisibleForTesting
    Observable<Batch<TargetStateBatchable, String>> events() {
        return engine.events();
    }

    /**
     * Temporary utility to backfill all targets from registered load balancers to the store, since targets were not
     * being persisted before.
     * <p>
     * Once all current state has been backfilled (i.e.: this runs successfully at least once), this is not needed
     * anymore and can be disabled/removed.
     */
    @VisibleForTesting
    void backfillTargetsToStore() {
        Observable<Single<LoadBalancer>> fetchLoadBalancerOperations = loadBalancerStore.getAssociations().stream()
                .map(a -> a.getJobLoadBalancer().getLoadBalancerId())
                .map(loadBalancerId -> loadBalancerConnector.getLoadBalancer(loadBalancerId)
                        // 404s will not error, and just return a LoadBalancer with state=REMOVED and no registered targets
                        .retryWhen(RetryHandlerBuilder.retryHandler()
                                .withRetryDelay(10, TimeUnit.MILLISECONDS)
                                .withRetryCount(10)
                                .buildExponentialBackoff()
                        )
                )
                .collect(Collectors.collectingAndThen(Collectors.toList(), Observable::from));

        try {
            int concurrency = configuration.getStoreBackfillConcurrencyLimit();
            ReactorExt.toFlux(Single.mergeDelayError(fetchLoadBalancerOperations, concurrency))
                    .flatMap(this::backfillTargetsToStore)
                    .ignoreElements()
                    .block(Duration.ofMillis(configuration.getStoreBackfillTimeoutMs()));
        } catch (Exception e) {
            Registry registry = runtime.getRegistry();
            registry.counter(registry.createId("titus.loadbalancer.service.backfillErrors",
                    "error", e.getClass().getSimpleName()
            )).increment();

            // swallow the error so we do not prevent activation of the leader
            // regular operations will not be affected by the incomplete backfill, but targets may leak until remaining
            // targets get backfilled during the next run
            logger.error("Backfill did not complete successfully, missing targets may be orphaned and leak on load balancers (i.e. never deregistered)", e);
        }
    }

    private Mono<Void> backfillTargetsToStore(LoadBalancer loadBalancer) {
        if (!loadBalancer.getState().equals(LoadBalancer.State.ACTIVE) ||
                loadBalancer.getRegisteredIps().isEmpty()) {
            return Mono.empty();
        }

        Set<LoadBalancerTargetState> targets = loadBalancer.getRegisteredIps().stream()
                .map(ip -> new LoadBalancerTargetState(
                        new LoadBalancerTarget(loadBalancer.getId(), "BACKFILLED", ip),
                        LoadBalancerTarget.State.REGISTERED
                ))
                .collect(Collectors.toSet());

        return loadBalancerStore.addOrUpdateTargets(targets);
    }
}
