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

package com.netflix.titus.ext.cassandra.store;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStoreException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder.LOAD_BALANCER_SANITIZER;

@Singleton
public class CassandraLoadBalancerStore implements LoadBalancerStore {
    private static Logger logger = LoggerFactory.getLogger(CassandraLoadBalancerStore.class);

    private static final String TABLE_LOAD_BALANCER_ASSOCIATIONS = "load_balancer_jobs";
    private static final String TABLE_LOAD_BALANCER_TARGETS = "load_balancer_targets";

    private static final String COLUMN_JOB_ID = "job_id";
    private static final String COLUMN_TASK_ID = "task_id";
    private static final String COLUMN_LOAD_BALANCER = "load_balancer_id";
    private static final String COLUMN_IP_ADDRESS = "ip_address";
    private static final String COLUMN_STATE = "state";

    private static final Integer FETCH_SIZE = Integer.MAX_VALUE;
    private static final long FETCH_TIMEOUT_MS = 120_000;

    private final PreparedStatement selectAssociations;
    private final PreparedStatement insertAssociation;
    private final PreparedStatement deleteAssociation;
    private final PreparedStatement selectTargetsForLoadBalancer;
    private final PreparedStatement insertTarget;
    private final PreparedStatement deleteDeregisteredTarget;

    private final CassandraStoreConfiguration configuration;

    private final EntitySanitizer entitySanitizer;

    private final Session session;
    private final CassStoreHelper storeHelper;

    /**
     * Stores a Job/Load Balancer's current state.
     */
    private final ConcurrentMap<JobLoadBalancer, JobLoadBalancer.State> loadBalancerStateMap;

    /**
     * Optimized index for lookups of associated JobLoadBalancers by Job ID.
     * Sets held in here must be all immutable (usually via Collections.unmodifiableSet).
     * Sets held here must be sorted to allow sorted page access (usually via JobID natural String ordering).
     */
    private final ConcurrentMap<String, SortedSet<JobLoadBalancer>> jobToAssociatedLoadBalancersMap;

    private static final String GET_ALL_ASSOCIATIONS = String
            .format("SELECT %s, %s, %s FROM %s;",
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER,
                    COLUMN_STATE,
                    TABLE_LOAD_BALANCER_ASSOCIATIONS);
    private static final String INSERT_ASSOCIATION = String
            .format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?);",
                    TABLE_LOAD_BALANCER_ASSOCIATIONS,
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER,
                    COLUMN_STATE);
    private static final String DELETE_ASSOCIATION = String
            .format("DELETE FROM %s WHERE %s = ? AND %s = ?",
                    TABLE_LOAD_BALANCER_ASSOCIATIONS,
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER);

    private static Pair<JobLoadBalancer, JobLoadBalancer.State> buildLoadBalancerStatePairFromRow(Row row) {
        String state = row.getString(COLUMN_STATE);
        return Pair.of(
                new JobLoadBalancer(row.getString(COLUMN_JOB_ID), row.getString(COLUMN_LOAD_BALANCER)),
                state == null ? JobLoadBalancer.State.DISSOCIATED : JobLoadBalancer.State.valueOf(state.toUpperCase())
        );
    }

    private static LoadBalancerTargetState buildLoadBalancerTargetStateFromRow(Row row) {
        return new LoadBalancerTargetState(
                new LoadBalancerTarget(
                        row.getString(COLUMN_LOAD_BALANCER),
                        row.getString(COLUMN_TASK_ID),
                        row.getString(COLUMN_IP_ADDRESS)
                ),
                LoadBalancerTarget.State.valueOf(row.getString(COLUMN_STATE))
        );
    }

    @Inject
    public CassandraLoadBalancerStore(CassandraStoreConfiguration configuration,
                                      @Named(LOAD_BALANCER_SANITIZER) EntitySanitizer entitySanitizer,
                                      Session session) {
        this.configuration = configuration;
        this.entitySanitizer = entitySanitizer;

        this.session = session;
        this.storeHelper = new CassStoreHelper(session);
        this.loadBalancerStateMap = new ConcurrentHashMap<>();
        this.jobToAssociatedLoadBalancersMap = new ConcurrentHashMap<>();

        this.selectAssociations = session.prepare(GET_ALL_ASSOCIATIONS).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        this.insertAssociation = session.prepare(INSERT_ASSOCIATION).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        this.deleteAssociation = session.prepare(DELETE_ASSOCIATION).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        this.selectTargetsForLoadBalancer = session.prepare(
                select(COLUMN_LOAD_BALANCER, COLUMN_IP_ADDRESS, COLUMN_TASK_ID, COLUMN_STATE)
                        .from(TABLE_LOAD_BALANCER_TARGETS)
                        .where(eq(COLUMN_LOAD_BALANCER, bindMarker()))
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        this.insertTarget = session.prepare(
                insertInto(TABLE_LOAD_BALANCER_TARGETS).values(
                        Arrays.asList(COLUMN_LOAD_BALANCER, COLUMN_IP_ADDRESS, COLUMN_TASK_ID, COLUMN_STATE),
                        Arrays.asList(bindMarker(), bindMarker(), bindMarker(), bindMarker())
                )
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        this.deleteDeregisteredTarget = session.prepare(
                delete().from(TABLE_LOAD_BALANCER_TARGETS)
                        .where(eq(COLUMN_LOAD_BALANCER, bindMarker()))
                        .and(eq(COLUMN_IP_ADDRESS, bindMarker()))
                        .onlyIf(eq(COLUMN_STATE, "DEREGISTERED"))
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    /**
     * Initialize the store from current C* data. Must be called prior to store usage.
     */
    @Activator
    public void init() {
        boolean failOnError = configuration.isFailOnInconsistentLoadBalancerData();
        loadAllAssociations(failOnError).block();
    }

    private Mono<Void> loadAllAssociations(boolean failOnError) {
        return ReactorExt.toFlux(storeHelper.execute(selectAssociations.bind().setFetchSize(FETCH_SIZE)))
                .timeout(Duration.ofMillis(FETCH_TIMEOUT_MS))
                .next()
                .flatMapMany(Flux::fromIterable)
                .map(CassandraLoadBalancerStore::buildLoadBalancerStatePairFromRow)
                .collect(Object::new, (ignored, loadBalancerStatePair) -> {
                    JobLoadBalancer jobLoadBalancer = loadBalancerStatePair.getLeft();
                    JobLoadBalancer.State state = loadBalancerStatePair.getRight();
                    Set<ValidationError> violations = entitySanitizer.validate(jobLoadBalancer);
                    if (violations.isEmpty()) {
                        loadBalancerStateMap.putIfAbsent(jobLoadBalancer, state);
                        SortedSet<JobLoadBalancer> jobLoadBalancers = jobToAssociatedLoadBalancersMap.getOrDefault(jobLoadBalancer.getJobId(), new TreeSet<>());
                        jobLoadBalancers.add(jobLoadBalancer);
                        jobToAssociatedLoadBalancersMap.put(jobLoadBalancer.getJobId(), jobLoadBalancers);
                    } else {
                        if (failOnError) {
                            throw LoadBalancerStoreException.badData(jobLoadBalancer, violations);
                        }
                        logger.warn("Ignoring bad association record of {} due to validation constraint violations: violations={}", jobLoadBalancer, violations);
                    }
                })
                .ignoreElement()
                .cast(Void.class);
    }

    /**
     * Returns an observable stream of the currently associated load balancers for a Job.
     *
     * @param jobId
     * @return
     */
    @Override
    public Observable<JobLoadBalancer> getAssociatedLoadBalancersForJob(String jobId) {
        return Observable.from(getAssociatedLoadBalancersSetForJob(jobId));
    }

    /**
     * This is in the critical path and should be fast, which is why it avoids lock contention, and keeps items indexed
     * by jobId yielding O(1).
     *
     * @param jobId
     * @return The current snapshot of what is currently being tracked
     */
    @Override
    public Set<JobLoadBalancer> getAssociatedLoadBalancersSetForJob(String jobId) {
        logger.debug("Getting all associated load balancers for job {}", jobId);
        return jobToAssociatedLoadBalancersMap.getOrDefault(jobId, Collections.emptySortedSet());
    }

    /**
     * Returns all current load balancer associations.
     *
     * @return
     */
    @Override
    public List<JobLoadBalancerState> getAssociations() {
        return loadBalancerStateMap.entrySet().stream()
                .map(JobLoadBalancerState::from)
                .collect(Collectors.toList());
    }


    @Override
    public List<JobLoadBalancer> getAssociationsPage(int offset, int limit) {
        // Create a sorted copy of the current keys to iterate. Keys added/removed after
        // the copy is created may lead to staleness in the data being iterated.
        // Use native string sorting to determine order.
        return jobToAssociatedLoadBalancersMap.keySet().stream()
                .flatMap(jobId -> {
                    SortedSet<JobLoadBalancer> jobLoadBalancerSortedSet = jobToAssociatedLoadBalancersMap.getOrDefault(jobId, Collections.emptySortedSet());
                    return jobLoadBalancerSortedSet.stream();
                })
                .sorted()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Marks the persisted and in-memory state as Dissociated and removes from association in-memory map.
     *
     * @param jobLoadBalancer
     * @param state
     * @return
     */
    @Override
    public Completable addOrUpdateLoadBalancer(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state) {
        logger.debug("Updating load balancer {} to state {}", jobLoadBalancer, state);
        return Completable.fromAction(() -> {
            synchronized (this) {
                BoundStatement stmt = insertAssociation.bind(jobLoadBalancer.getJobId(), jobLoadBalancer.getLoadBalancerId(), state.name());
                ResultSet rs = session.execute(stmt);
                loadBalancerStateMap.put(jobLoadBalancer, state);
                if (JobLoadBalancer.State.ASSOCIATED == state) {
                    addJobLoadBalancerAssociation(jobLoadBalancer);
                } else if (JobLoadBalancer.State.DISSOCIATED == state) {
                    removeJobLoadBalancerAssociation(jobLoadBalancer);
                }
            }
        });
    }

    /**
     * Removes the persisted Job/load balancer and state and removes in-memory state.
     *
     * @param jobLoadBalancer
     * @return
     */
    @Override
    public Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        logger.debug("Removing load balancer {}", jobLoadBalancer);
        BoundStatement stmt = deleteAssociation.bind(jobLoadBalancer.getJobId(), jobLoadBalancer.getLoadBalancerId());
        return storeHelper.execute(stmt)
                // Note: If the C* entry doesn't exist, it'll fail here and not remove from the map.
                .map(rs -> {
                    loadBalancerStateMap.remove(jobLoadBalancer);
                    removeJobLoadBalancerAssociation(jobLoadBalancer);
                    return jobLoadBalancer;
                })
                .toCompletable();
    }

    @Override
    public int getNumLoadBalancersForJob(String jobId) {
        int loadBalancerCount = 0;
        for (Map.Entry<JobLoadBalancer, JobLoadBalancer.State> entry : loadBalancerStateMap.entrySet()) {
            if (entry.getKey().getJobId().equals(jobId)) {
                loadBalancerCount++;
            }
        }
        return loadBalancerCount;
    }

    @Override
    public Mono<Void> addOrUpdateTargets(Collection<LoadBalancerTargetState> targets) {
        List<Mono<Void>> insertOperations = targets.stream()
                .map(this::addOrUpdateTarget)
                .collect(Collectors.toList());

        int limit = configuration.getLoadBalancerConcurrencyLimit();
        // prefetch does not matter here because operations don't produce any result (they are Mono<Void>)
        return Flux.mergeSequentialDelayError(insertOperations, limit, limit)
                .ignoreElements()
                .doOnSubscribe(ignored -> {
                    Map<String, Long> countPerLoadBalancer = targets.stream().collect(Collectors.groupingBy(
                            target -> target.getLoadBalancerTarget().getLoadBalancerId(),
                            Collectors.counting()
                    ));
                    logger.info("Inserting/updating targets: {}", countPerLoadBalancer);
                    logger.debug("Inserting/updating {} targets. Details: {}", targets.size(), targets);
                });
    }

    private Mono<Void> addOrUpdateTarget(LoadBalancerTargetState target) {
        Set<ValidationError> violations = entitySanitizer.validate(target);
        if (!violations.isEmpty()) {
            if (configuration.isFailOnInconsistentLoadBalancerData()) {
                return Mono.error(LoadBalancerStoreException.badData(target, violations));
            }
            logger.warn("Ignoring bad target record of {} due to validation constraint violations: violations={}", target, violations);
            return Mono.empty();
        }

        BoundStatement statement = insertTarget.bind(
                target.getLoadBalancerTarget().getLoadBalancerId(),
                target.getIpAddress(),
                target.getLoadBalancerTarget().getTaskId(),
                target.getState().name()
        );
        return ReactorExt.toMono(storeHelper.execute(statement).toCompletable());
    }

    @Override
    public Mono<Void> removeDeregisteredTargets(Collection<LoadBalancerTarget> toRemove) {
        List<Mono<Void>> deleteOperations = toRemove.stream()
                .map(target -> storeHelper.execute(
                        deleteDeregisteredTarget.bind(target.getLoadBalancerId(), target.getIpAddress())
                ).toCompletable())
                .map(ReactorExt::toMono)
                .collect(Collectors.toList());

        int limit = configuration.getLoadBalancerConcurrencyLimit();
        // prefetch does not matter here because operations don't produce any result (they are Mono<Void>)
        return Flux.mergeSequentialDelayError(deleteOperations, limit, limit)
                .ignoreElements()
                .doOnSubscribe(ignored -> logger.debug("Removing targets {}", toRemove));
    }

    @Override
    public Flux<LoadBalancerTargetState> getLoadBalancerTargets(String loadBalancerId) {
        Statement selectStmt = selectTargetsForLoadBalancer.bind(loadBalancerId).setFetchSize(FETCH_SIZE);
        return ReactorExt.toFlux(storeHelper.execute(selectStmt))
                .timeout(Duration.ofMillis(FETCH_TIMEOUT_MS))
                .next()
                .flatMapMany(Flux::fromIterable)
                .map(CassandraLoadBalancerStore::buildLoadBalancerTargetStateFromRow);
    }

    /**
     * Adds a new Job and associated Load Balancer by replacing any current set of associations
     * for the Job.
     *
     * @param association
     */
    private void addJobLoadBalancerAssociation(JobLoadBalancer association) {
        jobToAssociatedLoadBalancersMap.compute(association.getJobId(),
                (jobId, associations) -> {
                    if (associations == null) {
                        associations = new TreeSet<>();
                    }
                    // Add all of the current associations back, plus the new association
                    SortedSet<JobLoadBalancer> copy = new TreeSet<>(associations);
                    copy.add(association);

                    // Return the new, unmodifiable instance of the set.
                    return Collections.unmodifiableSortedSet(copy);
                }
        );
    }

    /**
     * Removes a Job's associated Load Balancer by replacing any current set of associations
     * for the Job.
     *
     * @param association
     */
    private void removeJobLoadBalancerAssociation(JobLoadBalancer association) {
        Supplier<TreeSet<JobLoadBalancer>> supplier = () -> new TreeSet<>();

        jobToAssociatedLoadBalancersMap.computeIfPresent(association.getJobId(),
                (jobId, associations) -> {
                    final SortedSet<JobLoadBalancer> copy = associations.stream()
                            .filter(entry -> !entry.equals(association))
                            .collect(Collectors.toCollection(supplier));
                    if (copy.isEmpty()) {
                        return null;
                    }
                    return Collections.unmodifiableSortedSet(copy);
                }
        );
    }
}
