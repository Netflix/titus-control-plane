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

import java.util.List;

import com.datastax.driver.core.Session;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.api.scheduler.store.SchedulerStore;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.model.scheduler.SchedulerGenerator;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Category(IntegrationTest.class)
public class CassandraSchedulerStoreTest {

    private static final long STARTUP_TIMEOUT = 30_000L;

    /**
     * As Cassandra uses memory mapped files there are sometimes issues with virtual disks storing the project files.
     * To solve this issue, we relocate the default embedded Cassandra folder to /var/tmp/embeddedCassandra.
     */
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT
    );

    private final CassandraStoreConfiguration configuration = mock(CassandraStoreConfiguration.class);

    private final EntitySanitizer entitySanitizer = mock(EntitySanitizer.class);

    private Session session;

    @Before
    public void setUp() {
        this.session = cassandraCQLUnit.getSession();
    }

    @Test
    public void testStoreAndRetrieveSystemSelectors() {
        List<SystemSelector> systemSelectors = CollectionsExt.merge(
                SchedulerGenerator.shouldSystemSelectors().toList(4),
                SchedulerGenerator.mustSystemSelectors().toList(3)
        );

        SchedulerStore bootstrappingTitusStore = createSchedulerStore();
        systemSelectors.forEach(systemSelector -> bootstrappingTitusStore.storeSystemSelector(systemSelector).await());

        SchedulerStore schedulerStore = createSchedulerStore();
        List<SystemSelector> result = schedulerStore.retrieveSystemSelectors().toList().toBlocking().first();
        assertThat(result).hasSize(7);
        assertThat(result).containsAll(systemSelectors);
    }

    @Test
    public void testDeleteSystemSelector() {
        List<SystemSelector> systemSelectors = CollectionsExt.merge(
                SchedulerGenerator.shouldSystemSelectors().toList(4),
                SchedulerGenerator.mustSystemSelectors().toList(3)
        );

        SchedulerStore schedulerStore = createSchedulerStore();
        systemSelectors.forEach(systemSelector -> schedulerStore.storeSystemSelector(systemSelector).await());

        assertThat(schedulerStore.retrieveSystemSelectors().toList().toBlocking().first()).hasSize(7);
        String idToDelete = systemSelectors.get(0).getId();
        schedulerStore.deleteSystemSelector(idToDelete).await();
        assertThat(schedulerStore.retrieveSystemSelectors().toList().toBlocking().first()).hasSize(6);
    }

    private CassandraSchedulerStore createSchedulerStore() {
        return new CassandraSchedulerStore(configuration, entitySanitizer, session);
    }
}