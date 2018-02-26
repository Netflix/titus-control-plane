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

package io.netflix.titus.ext.cassandra.store;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.api.scheduler.model.SystemSelector;
import io.netflix.titus.api.scheduler.store.SchedulerStore;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;
import static io.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static io.netflix.titus.common.util.guice.ProxyType.Logging;
import static io.netflix.titus.common.util.guice.ProxyType.Spectator;

@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class CassandraSchedulerStore implements SchedulerStore {

    private static final long TIMEOUT_MS = 60_000;

    // SELECT Queries
    private static final String RETRIEVE_SYSTEM_SELECTORS_STRING = "SELECT value FROM system_selectors;";

    // INSERT Queries
    private static final String INSERT_SYSTEM_SELECTOR_STRING = "INSERT INTO system_selectors (id, value) VALUES (?, ?);";

    // DELETE Queries
    private static final String DELETE_SYSTEM_SELECTOR_STRING = "DELETE FROM system_selectors WHERE id = ?";

    private final PreparedStatement retrieveSystemSelectorsStatement;
    private final PreparedStatement insertSystemSelectorStatement;
    private final PreparedStatement deleteSystemSelectorStatement;

    private final CassandraStoreConfiguration configuration;
    private final EntitySanitizer entitySanitizer;
    private final Session session;
    private final ObjectMapper mapper;

    @Inject
    public CassandraSchedulerStore(CassandraStoreConfiguration configuration,
                                   @Named(SCHEDULER_SANITIZER) EntitySanitizer entitySanitizer,
                                   Session session) {
        this(configuration, entitySanitizer, session, ObjectMappers.storeMapper());
    }

    public CassandraSchedulerStore(CassandraStoreConfiguration configuration,
                                   @Named(SCHEDULER_SANITIZER) EntitySanitizer entitySanitizer,
                                   Session session,
                                   ObjectMapper mapper) {
        this.configuration = configuration;
        this.entitySanitizer = entitySanitizer;
        this.session = session;
        this.mapper = mapper;

        retrieveSystemSelectorsStatement = session.prepare(RETRIEVE_SYSTEM_SELECTORS_STRING);
        insertSystemSelectorStatement = session.prepare(INSERT_SYSTEM_SELECTOR_STRING);
        deleteSystemSelectorStatement = session.prepare(DELETE_SYSTEM_SELECTOR_STRING);
    }

    @Activator
    public void enterActiveMode() {
        // We need this empty method, to mark this service as activated.
    }

    @Override
    public Observable<SystemSelector> retrieveSystemSelectors() {
        return StoreUtils.retrieve(
                session, mapper, retrieveSystemSelectorsStatement.bind(), SystemSelector.class, entitySanitizer, configuration.isFailOnInconsistentSchedulerData()
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable storeSystemSelector(SystemSelector systemSelector) {
        return StoreUtils.store(session,
                mapper,
                insertSystemSelectorStatement,
                systemSelector.getId(),
                systemSelector
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return StoreUtils.remove(
                session, deleteSystemSelectorStatement, id
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
