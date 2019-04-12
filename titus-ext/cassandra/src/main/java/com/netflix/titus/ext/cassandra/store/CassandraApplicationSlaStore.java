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

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.EntitySanitizers;
import rx.Observable;

@Singleton
public class CassandraApplicationSlaStore implements ApplicationSlaStore {

    private static final long TIMEOUT_MS = 60_000;

    // SELECT Queries
    private static final String RETRIEVE_CAPACITY_GROUPS_STRING = "SELECT value FROM \"%s\".\"TitusApplicationSLA\";";
    private static final String RETRIEVE_CAPACITY_GROUP_BY_NAME_STRING = "SELECT value FROM \"%s\".\"TitusApplicationSLA\" where key=? AND column1='applicationSLA';";

    // INSERT Queries
    private static final String INSERT_CAPACITY_GROUP_STRING = "INSERT INTO \"%s\".\"TitusApplicationSLA\" (key, column1, value) VALUES (?, 'applicationSLA', ?);";

    // DELETE Queries
    private static final String DELETE_CAPACITY_GROUP_STRING = "DELETE FROM \"%s\".\"TitusApplicationSLA\" WHERE key=?";

    private final CassandraStoreConfiguration configuration;
    private final Session session;

    private final ObjectMapper mapper = ObjectMappers.storeMapper();
    private final EntitySanitizer entitySanitizer = EntitySanitizers.alwaysValid();

    private final PreparedStatement retrieveCapacityGroupsStatement;
    private final PreparedStatement retrieveCapacityGroupByNameStatement;
    private final PreparedStatement insertCapacityGroupStatement;
    private final PreparedStatement deleteCapacityGroupStatement;

    @Inject
    public CassandraApplicationSlaStore(CassandraStoreConfiguration configuration,
                                        Session session) {
        this.configuration = configuration;
        this.session = session;

        String keySpace = configuration.getV2KeySpace();
        retrieveCapacityGroupsStatement = session.prepare(String.format(RETRIEVE_CAPACITY_GROUPS_STRING, keySpace));
        retrieveCapacityGroupByNameStatement = session.prepare(String.format(RETRIEVE_CAPACITY_GROUP_BY_NAME_STRING, keySpace));
        insertCapacityGroupStatement = session.prepare(String.format(INSERT_CAPACITY_GROUP_STRING, keySpace));
        deleteCapacityGroupStatement = session.prepare(String.format(DELETE_CAPACITY_GROUP_STRING, keySpace));
    }

    @Override
    public Observable<Void> create(ApplicationSLA applicationSLA) {
        return StoreUtils.store(session,
                mapper,
                insertCapacityGroupStatement,
                applicationSLA.getAppName(),
                applicationSLA
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).toObservable();
    }

    @Override
    public Observable<ApplicationSLA> findAll() {
        return StoreUtils.retrieve(
                session,
                mapper,
                retrieveCapacityGroupsStatement.bind(),
                ApplicationSLA.class,
                entitySanitizer,
                configuration.isFailOnInconsistentCapacityGroupData()
        );
    }

    @Override
    public Observable<ApplicationSLA> findByName(String applicationName) {
        return StoreUtils.retrieve(
                session,
                mapper,
                retrieveCapacityGroupByNameStatement.bind(applicationName),
                ApplicationSLA.class,
                entitySanitizer,
                configuration.isFailOnInconsistentCapacityGroupData()
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).take(1);
    }

    @Override
    public Observable<Void> remove(String applicationName) {
        return StoreUtils.remove(
                session, deleteCapacityGroupStatement, applicationName
        ).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).toObservable();
    }
}
