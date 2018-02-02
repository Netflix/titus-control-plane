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

import java.util.List;

import com.datastax.driver.core.Session;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroups;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Category(IntegrationTest.class)
public class CassandraAgentStoreTest {

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
    public void testStoreAndRetrieveAgentServerGroups() {
        List<AgentInstanceGroup> serverGroups = agentServerGroups().toList(2);

        AgentStore bootstrappingTitusStore = createAgentStore();
        serverGroups.forEach(agentServerGroup ->
                bootstrappingTitusStore.storeAgentInstanceGroup(agentServerGroup).get()
        );

        AgentStore agentStore = createAgentStore();
        List<AgentInstanceGroup> result = agentStore.retrieveAgentInstanceGroups().toList().toBlocking().first();
        assertThat(result).hasSize(2);
        assertThat(result).containsAll(serverGroups);
    }

    @Test
    public void testStoreAndRetrieveAgentInstances() {
        List<AgentInstance> instances = AgentGenerator.agentInstances().toList(2);

        AgentStore bootstrappingTitusStore = createAgentStore();
        instances.forEach(instance ->
                bootstrappingTitusStore.storeAgentInstance(instance).get()
        );

        AgentStore agentStore = createAgentStore();
        List<AgentInstance> result = agentStore.retrieveAgentInstances().toList().toBlocking().first();
        assertThat(result).hasSize(2);
        assertThat(result).containsAll(instances);
    }

    @Test
    public void testRemoveAgentServerGroup() {
        List<AgentInstanceGroup> serverGroups = agentServerGroups().toList(2);

        AgentStore agentStore = createAgentStore();
        serverGroups.forEach(sg -> agentStore.storeAgentInstanceGroup(sg).get());

        assertThat(agentStore.retrieveAgentInstanceGroups().toList().toBlocking().first()).hasSize(2);
        agentStore.removeAgentInstanceGroups(singletonList(serverGroups.get(0).getId())).get();
        assertThat(agentStore.retrieveAgentInstanceGroups().toList().toBlocking().first()).hasSize(1);
    }

    @Test
    public void testRemoveAgentInstance() {
        List<AgentInstance> instances = AgentGenerator.agentInstances().toList(2);

        AgentStore agentStore = createAgentStore();
        instances.forEach(instance -> agentStore.storeAgentInstance(instance).get());

        assertThat(agentStore.retrieveAgentInstances().toList().toBlocking().first()).hasSize(2);
        agentStore.removeAgentInstances(singletonList(instances.get(0).getId())).get();
        assertThat(agentStore.retrieveAgentInstances().toList().toBlocking().first()).hasSize(1);
    }

    private CassandraAgentStore createAgentStore() {
        return new CassandraAgentStore(configuration, entitySanitizer, session);
    }
}