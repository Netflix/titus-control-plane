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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

@IntegrationTest
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
            assertThat(store.getLoadBalancersForJob(jobLoadBalancer.getJobId()).toBlocking().getIterator().hasNext()).isFalse();
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
            store.getLoadBalancersForJob(jobLoadBalancer.getJobId()).subscribe(
                    loadBalancerState -> {
                        assertThat(testData.containsKey(new JobLoadBalancer(jobLoadBalancer.getJobId(), loadBalancerState.getLoadBalancerId()))).isTrue();
                        assertThat(loadBalancerState.getState()).isEqualTo(JobLoadBalancer.State.Dissociated);
                    });
        });
    }

    public class UpdateThread implements Runnable {
        private final JobLoadBalancer jobLoadBalancer;
        private final JobLoadBalancer.State state;
        private final CassandraLoadBalancerStore store;

        private UpdateThread(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state, CassandraLoadBalancerStore store) {
            this.jobLoadBalancer = jobLoadBalancer;
            this.state = state;
            this.store = store;
        }

        @Override
        public void run() {
            store.addOrUpdateLoadBalancer(jobLoadBalancer, state).await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testParallelUpdates() throws Exception {
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(100, 20);

        CassandraLoadBalancerStore store = getInitdStore();

        // Create an thread pool to generate concurrent updates
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        testData.forEach(
                (jobLoadBalancer, state) -> {
                    Runnable anUpdate = new UpdateThread(jobLoadBalancer, state, store);
                    executorService.execute(anUpdate);
                }
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
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(numTestJobs, numTestLbs);
        HashSet<JobLoadBalancer> unverifiedData = new HashSet<>(testData.keySet());

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
        int numTestJobs = 10;
        int numTestLbs = 20;
        Map<JobLoadBalancer, JobLoadBalancer.State> testData = generateTestData(numTestJobs, numTestLbs);

        // Load data on init
        loadTestData(testData);
        CassandraLoadBalancerStore store = getInitdStore();

        List<JobLoadBalancer> jobLoadBalancerPage = store.getAssociationsPage(0, (numTestJobs * numTestLbs) + 1);
        assertThat(jobLoadBalancerPage.size()).isEqualTo(numTestJobs * numTestLbs);
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
        Random random = new Random();

        for (int i = 1; i <= numJobs; i++) {
            String jobId = "Titus-" + random.nextInt(1000);
            for (int j = 1; j <= numLoadBalancersPerJob; j++) {
                JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, jobId + "-" + "TestLoadBalancer-" + random.nextInt(1000));
                while (testData.containsKey(jobLoadBalancer)) {
                    jobLoadBalancer = new JobLoadBalancer(jobId, jobId + "-" + "TestLoadBalancer-" + random.nextInt(1000));
                }
                assertThat(testData.put(jobLoadBalancer, JobLoadBalancer.State.Associated)).isNull();
            }
        }

        assertThat(testData.size()).isEqualTo(numJobs * numLoadBalancersPerJob);
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
        CassandraStoreConfiguration configuration = mock(CassandraStoreConfiguration.class);
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

        jobIdSet.forEach(jobId -> {
            // Verify we get the correct load balancers in the correct state
            store.getLoadBalancersForJob(jobId).subscribe(
                    loadBalancerState -> {
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
                                .isEqualTo(JobLoadBalancer.State.Associated);

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
}
