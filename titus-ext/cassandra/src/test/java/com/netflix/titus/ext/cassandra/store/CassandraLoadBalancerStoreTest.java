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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import org.assertj.core.util.IterableUtil;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.loadbalancer.model.JobLoadBalancer.State.ASSOCIATED;
import static com.netflix.titus.api.loadbalancer.model.JobLoadBalancer.State.DISSOCIATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(IntegrationNotParallelizableTest.class)
public class CassandraLoadBalancerStoreTest {
    private static Logger logger = LoggerFactory.getLogger(CassandraLoadBalancerStoreTest.class);

    private static final long STARTUP_TIMEOUT = 30_000L;
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";

    private static final long TIMEOUT_MS = 30_000;

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT
    );

    /**
     * Tests loading existing Cassandra data on store init.
     *
     * @throws Exception
     */
    @Test
    public void verifyStoreInit() throws Exception {
        loadTestData(generateTestData(5, 10, 1));
        assertThat(getInitdStore()).isNotNull();
    }

    /**
     * Tests retrieval of a data set loaded on initialization.
     *
     * @throws Exception
     */
    @Test
    public void testRetrieveLoadBalancers() throws Exception {
        TestData data = generateTestData(10, 20, 1);
        loadTestData(data);
        CassandraLoadBalancerStore store = getInitdStore();

        // Check that all expected data was loaded
        checkDataSetExists(store, data.getAssociations());
    }

    /**
     * Adds load balancers to jobs and checks that they were added properly.
     *
     * @throws Exception
     */
    @Test
    public void testAddLoadBalancers() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20, 1).getAssociations();
        CassandraLoadBalancerStore store = getInitdStore();

        // Apply the testData to the store
        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, state).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        // Check that all expected data was loaded
        checkDataSetExists(store, testData);
    }

    @Test
    public void testAssociationStateIsCaseInsensitive() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        PreparedStatement insertStmt = session.prepare("INSERT INTO load_balancer_jobs(job_id, load_balancer_id, state) VALUES(?, ?, ?);");

        ResultSet rs1 = session.execute(insertStmt.bind("job-1", "lb-1", "Associated"));
        assertThat(rs1.isExhausted()).isTrue();
        assertThat(rs1.wasApplied()).isTrue();

        ResultSet rs2 = session.execute(insertStmt.bind("job-2", "lb-2", "Dissociated"));
        assertThat(rs2.isExhausted()).isTrue();
        assertThat(rs2.wasApplied()).isTrue();

        ResultSet rs3 = session.execute(insertStmt.bind("job-3", "lb-3", "aSsOcIaTeD"));
        assertThat(rs3.isExhausted()).isTrue();
        assertThat(rs3.wasApplied()).isTrue();

        CassandraLoadBalancerStore store = getInitdStore();
        assertThat(store.getAssociations()).containsExactlyInAnyOrder(
                new JobLoadBalancerState(new JobLoadBalancer("job-1", "lb-1"), ASSOCIATED),
                new JobLoadBalancerState(new JobLoadBalancer("job-2", "lb-2"), DISSOCIATED),
                new JobLoadBalancerState(new JobLoadBalancer("job-3", "lb-3"), ASSOCIATED)
        );
    }

    @Test
    public void testRemoveLoadBalancer() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20, 1).getAssociations();
        CassandraLoadBalancerStore store = getInitdStore();

        testData.forEach((jobLoadBalancer, state) ->
                assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, state).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue()
        );

        testData.forEach((jobLoadBalancer, state) ->
                assertThat(store.removeLoadBalancer(jobLoadBalancer).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue()
        );

        Map<String, List<JobLoadBalancerState>> byJobId = store.getAssociations().stream()
                .collect(Collectors.groupingBy(JobLoadBalancerState::getJobId));
        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(byJobId.get(jobLoadBalancer.getJobId())).isNullOrEmpty();
        });
    }

    @Test
    public void testDissociateLoadBalancer() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20, 1).getAssociations();
        CassandraLoadBalancerStore store = getInitdStore();

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, ASSOCIATED).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, DISSOCIATED).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        Map<String, List<JobLoadBalancerState>> byJobId = store.getAssociations().stream()
                .collect(Collectors.groupingBy(JobLoadBalancerState::getJobId));
        testData.forEach((jobLoadBalancer, state) ->
                byJobId.get(jobLoadBalancer.getJobId()).forEach(loadBalancerState -> {
                    assertThat(testData).containsKey(new JobLoadBalancer(jobLoadBalancer.getJobId(), loadBalancerState.getLoadBalancerId()));
                    assertThat(loadBalancerState.getState()).isEqualTo(DISSOCIATED);
                })
        );
    }

    @Test(timeout = TIMEOUT_MS)
    public void testAddTargets() throws Exception {
        Map<LoadBalancerTarget, LoadBalancerTarget.State> testData = generateTestData(10, 20, 1).getTargets();
        Map<String, List<LoadBalancerTargetState>> expectedTargetsByLoadBalancer = testData.entrySet().stream()
                .map(entry -> new LoadBalancerTargetState(entry.getKey(), entry.getValue()))
                .collect(Collectors.groupingBy(t -> t.getLoadBalancerTarget().getLoadBalancerId()));
        CassandraLoadBalancerStore store = getInitdStore();
        store.addOrUpdateTargets(testData.entrySet().stream()
                .map(LoadBalancerTargetState::from)
                .collect(Collectors.toList())
        ).block();
        expectedTargetsByLoadBalancer.forEach((loadBalancerId, expectedTargets) ->
                assertThat(store.getLoadBalancerTargets(loadBalancerId).collectList().block())
                        .containsExactlyInAnyOrder(IterableUtil.toArray(expectedTargets))
        );

        int totalCount = expectedTargetsByLoadBalancer.values().stream().mapToInt(List::size).sum();
        Session session = cassandraCQLUnit.getSession();
        ResultSet resultSet = session.execute("SELECT COUNT(*) FROM load_balancer_targets;");
        assertThat(resultSet.one().getLong(0)).isEqualTo(totalCount);
    }

    @Test(timeout = TIMEOUT_MS)
    public void testUpdateTarget() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        BoundStatement countStmt = session.prepare("SELECT COUNT(*) FROM load_balancer_targets;").bind();
        PreparedStatement stateStmt = session.prepare("SELECT state FROM load_balancer_targets WHERE load_balancer_id = ? AND ip_address = ?;");

        assertThat(session.execute(countStmt).one().getLong(0)).isEqualTo(0);

        LoadBalancerTarget target = new LoadBalancerTarget("lb-1", "task-1", "1.1.1.1");
        CassandraLoadBalancerStore store = getInitdStore();
        store.addOrUpdateTargets(target.withState(LoadBalancerTarget.State.REGISTERED)).block();
        assertThat(session.execute(countStmt).one().getLong(0)).isEqualTo(1);
        Row registered = session.execute(stateStmt.bind("lb-1", "1.1.1.1")).one();
        assertThat(registered.getString("state")).isEqualTo("REGISTERED");

        store.addOrUpdateTargets(target.withState(LoadBalancerTarget.State.DEREGISTERED)).block();
        assertThat(session.execute(countStmt).one().getLong(0)).isEqualTo(1);
        Row deregistered = session.execute(stateStmt.bind("lb-1", "1.1.1.1")).one();
        assertThat(deregistered.getString("state")).isEqualTo("DEREGISTERED");
    }

    @Test
    public void testOnlyDeregisteredTargetsAreRemoved() throws Exception {
        Map<LoadBalancerTarget, LoadBalancerTarget.State> targets = ImmutableMap.of(
                new LoadBalancerTarget("lb-1", "task1", "1.1.1.1"), LoadBalancerTarget.State.REGISTERED,
                new LoadBalancerTarget("lb-1", "task2", "2.2.2.2"), LoadBalancerTarget.State.DEREGISTERED,
                new LoadBalancerTarget("lb-2", "task1", "1.1.1.1"), LoadBalancerTarget.State.DEREGISTERED,
                new LoadBalancerTarget("lb-2", "task3", "3.3.3.3"), LoadBalancerTarget.State.DEREGISTERED
        );
        loadTestData(new TestData(Collections.emptyMap(), targets));
        CassandraLoadBalancerStore store = getInitdStore();
        store.removeDeregisteredTargets(targets.keySet()).block(Duration.ofSeconds(10));
        List<LoadBalancerTargetState> targets1 = store.getLoadBalancerTargets("lb-1").collectList().block(Duration.ofSeconds(5));
        assertThat(targets1).hasSize(1);
        assertThat(targets1.get(0).getIpAddress()).isEqualTo("1.1.1.1");
        List<LoadBalancerTargetState> targets2 = store.getLoadBalancerTargets("lb-2").collectList().block(Duration.ofSeconds(5));
        assertThat(targets2).isEmpty();
    }

    @Test
    public void testParallelUpdates() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(100, 20, 1).getAssociations();

        CassandraLoadBalancerStore store = getInitdStore();

        // Create an thread pool to generate concurrent updates
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        testData.forEach((jobLoadBalancer, state) ->
                executorService.execute(() -> store.addOrUpdateLoadBalancer(jobLoadBalancer, state)
                        .await(TIMEOUT_MS, TimeUnit.MILLISECONDS))
        );
        // Wait till all jobs were submitted
        executorService.shutdown();
        assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // Verify data is consistent
        checkDataSetExists(store, testData);
    }

    /**
     * Tests that all data is returned across multiple pages.
     *
     * @throws Exception
     */
    @Test
    public void testGetPage() throws Exception {
        int numTestJobs = 100;
        int numTestLbs = 20;
        TestData testData = generateTestData(numTestJobs, numTestLbs, 1);
        Map<JobLoadBalancer, JobLoadBalancer.State> associations = testData.getAssociations();
        HashSet<JobLoadBalancer> unverifiedData = new HashSet<>(associations.keySet());

        // Load data on init
        loadTestData(testData);
        CassandraLoadBalancerStore store = getInitdStore();

        // Read little pages at a time until we're told we've read everything.
        int pageSize = 7;
        int currentPageOffset = 0;
        List<JobLoadBalancer> jobLoadBalancerPage;
        do {
            jobLoadBalancerPage = store.getAssociationsPage(currentPageOffset, pageSize);
            jobLoadBalancerPage.forEach(jobLoadBalancer -> {
                assertThat(unverifiedData.remove(jobLoadBalancer)).isTrue();
            });
            // Make sure all but the last page is full size
            if ((numTestJobs * numTestLbs) - currentPageOffset >= pageSize) {
                assertThat(jobLoadBalancerPage.size()).isEqualTo(pageSize);
            } else {
                assertThat(jobLoadBalancerPage.size()).isEqualTo((numTestJobs * numTestLbs) - currentPageOffset);
            }

            currentPageOffset += jobLoadBalancerPage.size();
            // Make sure we've stopped before reading beyond the data set size
            assertThat(currentPageOffset <= numTestJobs * numTestLbs).isTrue();
        } while (jobLoadBalancerPage.size() > 0);
        // Make sure all of the data was checked
        assertThat(unverifiedData.isEmpty()).isTrue();
    }

    /**
     * Tests that all data is returned in a single overly large page request.
     *
     * @throws Exception
     */
    @Test
    public void testGetFullPage() throws Exception {
        loadTestData(generateTestData(10, 20, 1));
        CassandraLoadBalancerStore store = getInitdStore();

        List<JobLoadBalancer> jobLoadBalancerPage = store.getAssociationsPage(0, (10 * 20) + 1);
        assertThat(jobLoadBalancerPage.size()).isEqualTo(10 * 20);
    }

    /**
     * Returns a map of data to be inserted that can be used for later verification.
     */
    private TestData generateTestData(int numJobs, int numLoadBalancersPerJob, int numTasksPerJob) {
        Map<JobLoadBalancer, JobLoadBalancer.State> associations = new HashMap<>();
        Map<LoadBalancerTarget, LoadBalancerTarget.State> targets = new HashMap<>();

        for (int i = 0; i < numJobs; i++) {
            String jobId = UUID.randomUUID().toString();
            for (int j = 0; j < numLoadBalancersPerJob; j++) {
                String loadBalancerId = UUID.randomUUID().toString();
                JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, jobId + "-" + loadBalancerId);
                associations.put(jobLoadBalancer, ASSOCIATED);
                for (int t = 0; t < numTasksPerJob; t++) {
                    targets.put(new LoadBalancerTarget(
                            loadBalancerId, "task-" + t, String.format("%s.%s.%s.%s", i, t, t, t)
                    ), LoadBalancerTarget.State.REGISTERED);
                }
            }
        }

        assertThat(associations.size()).isEqualTo(numJobs * numLoadBalancersPerJob);
        assertThat(targets.size()).isEqualTo(numJobs * numLoadBalancersPerJob * numTasksPerJob);
        return new TestData(associations, targets);
    }

    private void loadTestData(Pair<Map<JobLoadBalancer, JobLoadBalancer.State>, Map<LoadBalancerTarget, LoadBalancerTarget.State>> data) {
        loadTestData(data.getLeft(), data.getRight());
    }

    /**
     * Inserts data in C* for use during tests.
     */
    private void loadTestData(Map<JobLoadBalancer, JobLoadBalancer.State> associations, Map<LoadBalancerTarget,
            LoadBalancerTarget.State> targets) {
        Session session = cassandraCQLUnit.getSession();
        PreparedStatement associationStmt = session.prepare("INSERT INTO load_balancer_jobs(job_id, load_balancer_id, state) VALUES(?, ?, ?);");
        PreparedStatement targetStmt = session.prepare("INSERT INTO load_balancer_targets(load_balancer_id, ip_address, task_id, state) VALUES(?, ?, ?, ?);");

        associations.forEach((jobLoadBalancer, state) -> {
            BoundStatement boundStatement = associationStmt.bind(
                    jobLoadBalancer.getJobId(),
                    jobLoadBalancer.getLoadBalancerId(),
                    state.name());
            ResultSet rs = session.execute(boundStatement);
            assertThat(rs.isExhausted()).isTrue();
            assertThat(rs.wasApplied()).isTrue();
        });

        targets.forEach((target, state) -> {
            BoundStatement boundStatement = targetStmt.bind(
                    target.getLoadBalancerId(),
                    target.getIpAddress(),
                    target.getTaskId(),
                    state.name());
            ResultSet rs = session.execute(boundStatement);
            assertThat(rs.isExhausted()).isTrue();
            assertThat(rs.wasApplied()).isTrue();
        });
    }

    /**
     * Creates, loads, and returns a store instance based on what was already in Cassandra.
     */
    private CassandraLoadBalancerStore getInitdStore() {
        Session session = cassandraCQLUnit.getSession();
        CassandraStoreConfiguration configuration = mock(CassandraStoreConfiguration.class);
        when(configuration.getLoadBalancerWriteConcurrencyLimit()).thenReturn(10);
        when(configuration.getLoadBalancerDeleteConcurrencyLimit()).thenReturn(1);
        EntitySanitizer entitySanitizer = new LoadBalancerSanitizerBuilder().build();
        CassandraLoadBalancerStore store = new CassandraLoadBalancerStore(configuration, entitySanitizer, session);
        store.init();

        return store;
    }

    /**
     * Returns the set of JobIds in a test data map.
     *
     * @return
     */
    private Set<String> getJobIdsFromTestData(Map<JobLoadBalancer, JobLoadBalancer.State> testData) {
        Set<String> jobIdSet = new HashSet<>();
        testData.keySet()
                .forEach(jobLoadBalancer -> jobIdSet.add(jobLoadBalancer.getJobId()));
        return jobIdSet;
    }

    /**
     * Checks if a provided data set fully exists in the store. The method is
     * expected to assert if the check is false.
     *
     * @throws Exception
     */
    private void checkDataSetExists(CassandraLoadBalancerStore store, Map<JobLoadBalancer, JobLoadBalancer.State> testData) throws Exception {
        Set<JobLoadBalancer> observableVerificationSet = new HashSet<>(testData.keySet());
        Set<JobLoadBalancer> listVerificationSet = new HashSet<>(observableVerificationSet);
        Set<String> jobIdSet = getJobIdsFromTestData(testData);

        Map<String, List<JobLoadBalancerState>> byJobId = store.getAssociations().stream()
                .collect(Collectors.groupingBy(JobLoadBalancerState::getJobId));
        jobIdSet.forEach(jobId -> {
            // Verify we get the correct load balancers in the correct state
            byJobId.get(jobId)
                    .forEach(loadBalancerState -> {
                        // Verify that all of the returned data was in the test data.
                        JobLoadBalancer jobLoadBalancer = loadBalancerState.getJobLoadBalancer();
                        assertThat(jobLoadBalancer.getJobId().equals(jobId)).isTrue();
                        assertThat(testData.containsKey(jobLoadBalancer)).isTrue();
                        assertThat(testData.get(jobLoadBalancer))
                                .isEqualTo(loadBalancerState.getState());

                        // Mark that this job/load balancer was checked
                        assertThat(observableVerificationSet.contains(jobLoadBalancer)).isTrue();
                        assertThat(observableVerificationSet.remove(jobLoadBalancer)).isTrue();
                        logger.debug("Verified job {} has load balancer id {} in state {}",
                                jobId,
                                loadBalancerState.getLoadBalancerId(),
                                loadBalancerState.getState());
                    });

            // Verify the secondary indexes return the correct state
            store.getAssociatedLoadBalancersSetForJob(jobId)
                    .forEach(jobLoadBalancer -> {
                        logger.info("Verifying jobLoadBalancer {}", jobLoadBalancer);
                        assertThat(jobLoadBalancer.getJobId().equals(jobId)).isTrue();
                        assertThat(testData.containsKey(jobLoadBalancer)).isTrue();
                        assertThat(testData.get(jobLoadBalancer))
                                .isEqualTo(ASSOCIATED);

                        // Mark that this job/load balancer was checked
                        assertThat(listVerificationSet.contains(jobLoadBalancer)).isTrue();
                        assertThat(listVerificationSet.remove(jobLoadBalancer)).isTrue();
                        logger.debug("Verified job load balancer {}", jobLoadBalancer);
                    });
        });

        // Verify that all of the test data was checked.
        assertThat(observableVerificationSet.isEmpty()).isTrue();
        assertThat(listVerificationSet.isEmpty()).isTrue();
    }

    /**
     * Generics sanity
     */
    private static class TestData extends Pair<Map<JobLoadBalancer, JobLoadBalancer.State>, Map<LoadBalancerTarget, LoadBalancerTarget.State>> {
        public TestData(Map<JobLoadBalancer, JobLoadBalancer.State> associations, Map<LoadBalancerTarget, LoadBalancerTarget.State> targets) {
            super(associations, targets);
        }

        public Map<JobLoadBalancer, JobLoadBalancer.State> getAssociations() {
            return getLeft();
        }

        public Map<LoadBalancerTarget, LoadBalancerTarget.State> getTargets() {
            return getRight();
        }
    }
}
