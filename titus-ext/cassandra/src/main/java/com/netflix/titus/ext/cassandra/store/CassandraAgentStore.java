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
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder.AGENT_SANITIZER;
import static com.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static com.netflix.titus.common.util.guice.ProxyType.Logging;
import static com.netflix.titus.common.util.guice.ProxyType.Spectator;

@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class CassandraAgentStore implements AgentStore {

    private static final long TIMEOUT_MS = 60_000;

    // SELECT Queries
    private static final String RETRIEVE_AGENT_INSTANCE_GROUPS_STRING = "SELECT value FROM agent_instance_groups;";
    private static final String RETRIEVE_AGENT_INSTANCES_STRING = "SELECT value FROM agent_instances;";

    // INSERT Queries
    private static final String INSERT_AGENT_INSTANCE_GROUP_STRING = "INSERT INTO agent_instance_groups (id, value) VALUES (?, ?);";
    private static final String INSERT_AGENT_INSTANCE_STRING = "INSERT INTO agent_instances (id, value) VALUES (?, ?);";

    // DELETE Queries
    private static final String DELETE_AGENT_INSTANCE_GROUPS_STRING = "DELETE FROM agent_instance_groups WHERE id IN ?";

    private static final String DELETE_AGENT_INSTANCE_STRING = "DELETE FROM agent_instances WHERE id IN ?";

    private final PreparedStatement retrieveInstanceGroupsStatement;
    private final PreparedStatement retrieveInstancesStatement;

    private final PreparedStatement insertAgentInstanceGroupStatement;
    private final PreparedStatement insertAgentInstanceStatement;

    private final PreparedStatement deleteAgentInstanceGroupStatement;
    private final PreparedStatement deleteAgentInstanceStatement;

    private final CassandraStoreConfiguration configuration;
    private final EntitySanitizer entitySanitizer;
    private final Session session;
    private final ObjectMapper mapper;

    @Inject
    public CassandraAgentStore(CassandraStoreConfiguration configuration,
                               @Named(AGENT_SANITIZER) EntitySanitizer entitySanitizer,
                               Session session) {
        this(configuration, entitySanitizer, session, ObjectMappers.storeMapper());
    }

    public CassandraAgentStore(CassandraStoreConfiguration configuration,
                               @Named(AGENT_SANITIZER) EntitySanitizer entitySanitizer,
                               Session session,
                               ObjectMapper mapper) {
        this.configuration = configuration;
        this.entitySanitizer = entitySanitizer;
        this.session = session;
        this.mapper = mapper;

        retrieveInstanceGroupsStatement = session.prepare(RETRIEVE_AGENT_INSTANCE_GROUPS_STRING);
        retrieveInstancesStatement = session.prepare(RETRIEVE_AGENT_INSTANCES_STRING);
        insertAgentInstanceGroupStatement = session.prepare(INSERT_AGENT_INSTANCE_GROUP_STRING);
        insertAgentInstanceStatement = session.prepare(INSERT_AGENT_INSTANCE_STRING);
        deleteAgentInstanceGroupStatement = session.prepare(DELETE_AGENT_INSTANCE_GROUPS_STRING);
        deleteAgentInstanceStatement = session.prepare(DELETE_AGENT_INSTANCE_STRING);
    }

    @Activator
    public void enterActiveMode() {
        // We need this empty method, to mark this service as activated.
    }

    @Override
    public Observable<AgentInstanceGroup> retrieveAgentInstanceGroups() {
        return StoreUtils.retrieve(
                session, mapper, retrieveInstanceGroupsStatement.bind(), AgentInstanceGroup.class, entitySanitizer, configuration.isFailOnInconsistentAgentData()
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<AgentInstance> retrieveAgentInstances() {
        return StoreUtils.retrieve(
                session, mapper, retrieveInstancesStatement.bind(), AgentInstance.class, entitySanitizer, configuration.isFailOnInconsistentAgentData()
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable storeAgentInstanceGroup(AgentInstanceGroup agentInstanceGroup) {
        return StoreUtils.store(session,
                mapper,
                insertAgentInstanceGroupStatement,
                agentInstanceGroup.getId(),
                agentInstanceGroup
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable storeAgentInstance(AgentInstance agentInstance) {
        return StoreUtils.store(session,
                mapper,
                insertAgentInstanceStatement,
                agentInstance.getId(),
                agentInstance
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable removeAgentInstanceGroups(List<String> agentInstanceGroupIds) {
        return StoreUtils.remove(
                session, deleteAgentInstanceGroupStatement, agentInstanceGroupIds
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable removeAgentInstances(List<String> agentInstanceIds) {
        return StoreUtils.remove(
                session, deleteAgentInstanceStatement, agentInstanceIds
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
