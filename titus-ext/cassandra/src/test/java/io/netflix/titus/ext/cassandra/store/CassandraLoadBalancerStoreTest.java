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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CassandraLoadBalancerStoreTest {
    private static Logger log = LoggerFactory.getLogger(CassandraLoadBalancerStoreTest.class);

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
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(5, 10);

        loadTestData(testData);
        assertThat(getInitdStore()).isNotNull();
    }

    /**
     * Tests retrieval of a data set loaded on initialization.
     *
     * @throws Exception
     */
    @Test
    public void testRetrieveLoadBalancers() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20);

        // Load data on init
        loadTestData(testData);
        CassandraLoadBalancerStore store = getInitdStore();

        // Check that all expected data was loaded
        checkDataSetExists(store, testData);
    }

    /**
     * Adds load balancers to jobs and checks that they were added properly.
     *
     * @throws Exception
     */
    @Test
    public void testAddLoadBalancers() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20);
        CassandraLoadBalancerStore store = getInitdStore();

        // Apply the testData to the store
        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, state).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        // Check that all expected data was loaded
        checkDataSetExists(store, testData);
    }

    @Test
    public void testRemoveLoadBalancer() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20);
        CassandraLoadBalancerStore store = getInitdStore();

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, state).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.removeLoadBalancer(jobLoadBalancer).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.retrieveLoadBalancersForJob(jobLoadBalancer.getJobId()).toBlocking().getIterator().hasNext()).isFalse();
        });
    }

    @Test
    public void testDissociateLoadBalancer() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(10, 20);
        CassandraLoadBalancerStore store = getInitdStore();

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        testData.forEach((jobLoadBalancer, state) -> {
            assertThat(store.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Dissociated).await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
        });

        testData.forEach((jobLoadBalancer, state) -> {
            store.retrieveLoadBalancersForJob(jobLoadBalancer.getJobId()).subscribe(
                    pair -> {
                        assertThat(testData.containsKey(new JobLoadBalancer(jobLoadBalancer.getJobId(), pair.getLeft()))).isTrue();
                        assertThat(pair.getRight()).isEqualTo(JobLoadBalancer.State.Dissociated);
                    });
        });
    }

    /**
     * Returns a map of data to be inserted that can be used for later verification.
     *
     * @param numJobs
     * @param numLoadBalancersPerJob
     * @throws Exception
     */
    private Map<JobLoadBalancer, JobLoadBalancer.State> generateTestData(int numJobs, int numLoadBalancersPerJob) throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = new ConcurrentHashMap<>();

        for (int i = 1; i <= numJobs; i++) {
            String jobId = "Titus-" + i;
            for (int j = 1; j <= numLoadBalancersPerJob; j++) {
                testData.putIfAbsent(new JobLoadBalancer(jobId, jobId + "-" + "TestLoadBalancer-" + j), JobLoadBalancer.State.Associated);
            }
        }

        return testData;
    }

    /**
     * Inserts data in C* for use during tests.
     *
     * @throws Exception
     */
    private void loadTestData(Map<JobLoadBalancer, JobLoadBalancer.State> testData) throws Exception {
        Session session = cassandraCQLUnit.getSession();
        String insertStmt = "INSERT INTO load_balancer_jobs(job_id, load_balancer_id, state) VALUES(?, ?, ?);";
        PreparedStatement stmt = session.prepare(insertStmt);

        testData.forEach((jobLoadBalancer, state) -> {
            BoundStatement boundStatement = stmt.bind(
                    jobLoadBalancer.getJobId(),
                    jobLoadBalancer.getLoadBalancerId(),
                    state.name());
            ResultSet rs = session.execute(boundStatement);
            assertThat(rs.isExhausted()).isTrue();
            assertThat(rs.wasApplied()).isTrue();
        });
    }

    /**
     * Creates, loads, and returns a store instance based on what was already in Cassandra.
     *
     * @return
     * @throws Exception
     */
    private CassandraLoadBalancerStore getInitdStore() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        CassandraLoadBalancerStore store = new CassandraLoadBalancerStore(session, new DefaultRegistry());
        store.init();

        return store;
    }

    /**
     * Checks if a provided data set fully exists in the store. The method is
     * expected to assert if the check is false.
     *
     * @throws Exception
     */
    private void checkDataSetExists(CassandraLoadBalancerStore store, Map<JobLoadBalancer, JobLoadBalancer.State> testData) throws Exception {
        testData.keySet().stream()
                .map(jobLoadBalancer -> jobLoadBalancer.getJobId())
                .collect(Collectors.toSet())
                .forEach(jobId -> {
                    store.retrieveLoadBalancersForJob(jobId).subscribe(
                            pair -> {
                                log.info("Got back pair {} for job {}", pair, jobId);
                                assertThat(testData.containsKey(new JobLoadBalancer(jobId, pair.getLeft()))).isTrue();
                                assertThat(testData.get(new JobLoadBalancer(jobId, pair.getLeft()))).isEqualTo(pair.getRight());
                                log.info("Verified job {} has load balancer id {} in state {}", jobId, pair.getLeft(), pair.getRight());
                            });
                });
    }
}
