/*
 * Copyright 2019 Netflix, Inc.
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

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Session;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(IntegrationNotParallelizableTest.class)
public class CassandraApplicationSlaStoreTest {

    private static final long STARTUP_TIMEOUT = 30_000L;

    private static final long TIMEOUT = 30_000L;

    /**
     * As Cassandra uses memory mapped files there are sometimes issues with virtual disks storing the project files.
     * To solve this issue, we relocate the default embedded Cassandra folder to /var/tmp/embeddedCassandra.
     */
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";

    private static final String TEST_KEYSPACE = "titus_integration_tests";

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("capacity_group_table.cql", TEST_KEYSPACE),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT
    );

    private final CassandraStoreConfiguration configuration = mock(CassandraStoreConfiguration.class);

    private Session session;

    @Before
    public void setUp() {
        this.session = cassandraCQLUnit.getSession();
        when(configuration.getV2KeySpace()).thenReturn(TEST_KEYSPACE);
    }

    @Test
    public void testStoreAndRetrieveCapacityGroups() {
        ApplicationSLA capacityGroup1 = ApplicationSlaSample.DefaultFlex.build();
        ApplicationSLA capacityGroup2 = ApplicationSlaSample.CriticalLarge.build();
        List<ApplicationSLA> capacityGroups = Arrays.asList(capacityGroup1, capacityGroup2);

        // Create initial
        ApplicationSlaStore bootstrappingTitusStore = createStore();
        capacityGroups.forEach(capacityGroup ->
                assertThat(bootstrappingTitusStore.create(capacityGroup).toCompletable().await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue()
        );

        // Read all
        ApplicationSlaStore store = createStore();
        List<ApplicationSLA> result = store.findAll().toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).hasSize(2);
        assertThat(result).containsAll(capacityGroups);

        // Find by name
        ApplicationSLA fetched = store.findByName(capacityGroup1.getAppName()).timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(fetched).isEqualTo(capacityGroup1);

        // Now delete
        assertThat(store.remove(capacityGroup1.getAppName()).toCompletable().await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue();

        try {
            store.findByName(capacityGroup1.getAppName()).timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
            Assert.fail();
        } catch (NoSuchElementException expected) {
        }
    }

    private ApplicationSlaStore createStore() {
        return new CassandraApplicationSlaStore(configuration, session);
    }
}