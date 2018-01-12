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

package io.netflix.titus.ext.cassandra.store;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStoreException;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

@Singleton
public class CassandraLoadBalancerStore implements LoadBalancerStore {
    private static Logger logger = LoggerFactory.getLogger(CassandraLoadBalancerStore.class);

    private static final String TABLE_LOAD_BALANCER = "load_balancer_jobs";
    private static final String COLUMN_JOB_ID = "job_id";
    private static final String COLUMN_LOAD_BALANCER = "load_balancer_id";
    private static final String COLUMN_STATE = "state";

    private static final Integer FETCH_SIZE = Integer.MAX_VALUE;
    private static final long FETCH_TIMEOUT_MS = 120_000;

    private final PreparedStatement getAllJobIdsStmt;
    private final PreparedStatement insertLoadBalancerStmt;
    private final PreparedStatement deleteLoadBalancerStmt;

    private final CassandraStoreConfiguration configuration;

    private final EntitySanitizer entitySanitizer;

    private final Session session;
    private final CassStoreHelper storeHelper;

    /**
     * Stores a Job/Load Balancer's current state.
     */
    private volatile ConcurrentMap<JobLoadBalancer, JobLoadBalancer.State> loadBalancerStateMap;

    /**
     * Optimized index for lookups of associated JobLoadBalancers by Job ID.
     * Sets held in here must be all immutable (usually via Collections.unmodifiableSet).
     * Sets held here must be sorted to allow sorted page access (usually via JobID natural String ordering).
     */
    private final ConcurrentMap<String, SortedSet<JobLoadBalancer>> jobToAssociatedLoadBalancersMap;

    private static final String GET_ALL_JOB_IDS = String
            .format("SELECT %s, %s, %s FROM %s;",
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER,
                    COLUMN_STATE,
                    TABLE_LOAD_BALANCER);
    private static final String INSERT_JOB_LOAD_BALANCER = String
            .format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?);",
                    TABLE_LOAD_BALANCER,
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER,
                    COLUMN_STATE);
    private static final String UPDATE_JOB_LOAD_BALANCER_STATE = String
            .format("UPDATE %s SET %s = ? WHERE %s = ? AND %s = ?;",
                    TABLE_LOAD_BALANCER,
                    COLUMN_STATE,
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER);
    private static final String DELETE_JOB_LOAD_BALANCER = String
            .format("DELETE FROM %s WHERE %s = ? AND %s = ?",
                    TABLE_LOAD_BALANCER,
                    COLUMN_JOB_ID,
                    COLUMN_LOAD_BALANCER);

    @Inject
    public CassandraLoadBalancerStore(CassandraStoreConfiguration configuration, @Named(LOAD_BALANCER_SANITIZER) EntitySanitizer entitySanitizer, Session session) {
        this.configuration = configuration;
        this.entitySanitizer = entitySanitizer;

        this.session = session;
        this.storeHelper = new CassStoreHelper(session);
        this.loadBalancerStateMap = new ConcurrentHashMap<>();
        this.jobToAssociatedLoadBalancersMap = new ConcurrentHashMap<>();

        this.getAllJobIdsStmt = session.prepare(GET_ALL_JOB_IDS);
        this.insertLoadBalancerStmt = session.prepare(INSERT_JOB_LOAD_BALANCER);
        this.deleteLoadBalancerStmt = session.prepare(DELETE_JOB_LOAD_BALANCER);
    }

    /**
     * Initialize the store from current C* data. Must be called prior to store usage.
     */
    @Activator
    public void init() {
        boolean failOnError = configuration.isFailOnInconsistentLoadBalancerData();

        storeHelper.execute(getAllJobIdsStmt.bind().setFetchSize(FETCH_SIZE))
                .timeout(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .flatMap(rows -> Observable.from(rows.all()))
                .map(this::buildLoadBalancerStatePairFromRow)
                .toBlocking()
                .forEach(loadBalancerStatePair -> {
                    JobLoadBalancer jobLoadBalancer = loadBalancerStatePair.getLeft();
                    JobLoadBalancer.State state = loadBalancerStatePair.getRight();
                    Set<ConstraintViolation<JobLoadBalancer>> violations = entitySanitizer.validate(jobLoadBalancer);
                    if (violations.isEmpty()) {
                        loadBalancerStateMap.putIfAbsent(jobLoadBalancer, state);
                        SortedSet<JobLoadBalancer> jobLoadBalancers = jobToAssociatedLoadBalancersMap.getOrDefault(jobLoadBalancer.getJobId(), new TreeSet<>());
                        jobLoadBalancers.add(jobLoadBalancer);
                        jobToAssociatedLoadBalancersMap.put(jobLoadBalancer.getJobId(), jobLoadBalancers);
                    } else {
                        if (failOnError) {
                            throw LoadBalancerStoreException.badData(jobLoadBalancer, violations);
                        }
                        logger.warn("Ignoring bad record of {} due to validation constraint violations: violations={}", jobLoadBalancer, violations);
                    }
                });
    }

    @Override
    public Observable<JobLoadBalancerState> getLoadBalancersForJob(String jobId) {
        logger.debug("Getting all load balancer states for job {}", jobId);
        return Observable.from(loadBalancerStateMap.entrySet())
                .filter(entry -> entry.getKey().getJobId().equals(jobId))
                .map(JobLoadBalancerState::from);
    }

    /**
     * Returns an observable stream of the currently associated load balancers for a Job.
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
        return  jobToAssociatedLoadBalancersMap.getOrDefault(jobId, Collections.emptySortedSet());
    }

    /**
     * Returns all current load balancer associations.
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
        // the copy is created may lead to staleness in the date being iterated.
        // Use native string sorting to determine order.
        return jobToAssociatedLoadBalancersMap.keySet().stream()
                .sorted()
                .flatMap(jobId -> {
                    SortedSet<JobLoadBalancer> jobLoadBalancerSortedSet = jobToAssociatedLoadBalancersMap.getOrDefault(jobId, Collections.emptySortedSet());
                    return jobLoadBalancerSortedSet.stream();
                })
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Marks the persisted and in-memory state as Dissociated and removes from association in-memory map.
     * @param jobLoadBalancer
     * @param state
     * @return
     */
    @Override
    public Completable addOrUpdateLoadBalancer(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state) {
        logger.debug("Updating load balancer {} to state {}", jobLoadBalancer, state);
        return Completable.fromAction(() -> {
            synchronized (this) {
                BoundStatement stmt = insertLoadBalancerStmt.bind(jobLoadBalancer.getJobId(), jobLoadBalancer.getLoadBalancerId(), state.name());
                ResultSet rs = session.execute(stmt);
                loadBalancerStateMap.put(jobLoadBalancer, state);
                if (JobLoadBalancer.State.Associated == state) {
                    addJobLoadBalancerAssociation(jobLoadBalancer);
                } else if (JobLoadBalancer.State.Dissociated == state) {
                    removeJobLoadBalancerAssociation(jobLoadBalancer);
                }
            }
        });
    }

    /**
     * Removes the persisted Job/load balancer and state and removes in-memory state.
     * @param jobLoadBalancer
     * @return
     */
    @Override
    public Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        logger.debug("Removing load balancer {}", jobLoadBalancer);
        BoundStatement stmt = deleteLoadBalancerStmt.bind(jobLoadBalancer.getJobId(), jobLoadBalancer.getLoadBalancerId());
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

    private Pair<JobLoadBalancer, JobLoadBalancer.State> buildLoadBalancerStatePairFromRow(Row row) {
        return Pair.of(new JobLoadBalancer(row.getString(COLUMN_JOB_ID), row.getString(COLUMN_LOAD_BALANCER)),
                JobLoadBalancer.State.valueOf(row.getString(COLUMN_STATE)));
    }

    /**
     * Adds a new Job and associated Load Balancer by replacing any current set of associations
     * for the Job.
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
