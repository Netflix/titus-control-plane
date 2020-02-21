/*
 * Copyright 2020 Netflix, Inc.
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.assertj.core.api.Assertions.assertThat;

public class CassStoreHelperTest {

    private static final long STARTUP_TIMEOUT = 60_000L;

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

    @Test
    public void paginatedResultSetCanBeFetchedAsync() {
        int numberOfRecords = 500;
        int numberOfPages = 10;
        Session session = cassandraCQLUnit.getSession();
        PreparedStatement insertStmt = session.prepare(insertInto("app_scale_jobs").values(
                Arrays.asList("job_id", "ref_id"),
                Arrays.asList(bindMarker(), bindMarker())
        ));

        for (int i = 0; i < numberOfRecords; i++) {
            ResultSet resultSet = session.execute(insertStmt.bind("job-" + i, UUID.randomUUID()));
            assertThat(resultSet.wasApplied()).isTrue();
        }

        PreparedStatement loadStmt = session.prepare(select("job_id", "ref_id").from("app_scale_jobs"));
        Observable<ResultSet> results = new CassStoreHelper(session, Schedulers.io()).execute(loadStmt.bind()
                // force pagination, and pages to be fetched on demand as the ResultSet is iterated on
                .setFetchSize(numberOfRecords / numberOfPages));

        results.doOnNext(rows -> rows.forEach(row ->
                assertThat(row.getString(0)).startsWith("job-"))
        ).toCompletable().await(1, TimeUnit.MINUTES);
    }

    @Test(expected = IllegalStateException.class /* the datastax driver complains that page fetching will cause a deadlock */ )
    public void paginatedResultInCassandraThreadsThrowsException() {
        int numberOfRecords = 500;
        int numberOfPages = 10;
        Session session = cassandraCQLUnit.getSession();
        PreparedStatement insertStmt = session.prepare(insertInto("app_scale_jobs").values(
                Arrays.asList("job_id", "ref_id"),
                Arrays.asList(bindMarker(), bindMarker())
        ));

        for (int i = 0; i < numberOfRecords; i++) {
            ResultSet resultSet = session.execute(insertStmt.bind("job-" + i, UUID.randomUUID()));
            assertThat(resultSet.wasApplied()).isTrue();
        }

        PreparedStatement loadStmt = session.prepare(select("job_id", "ref_id").from("app_scale_jobs"));
        Observable<ResultSet> results = new CassStoreHelper(session, Schedulers.immediate()).execute(loadStmt.bind()
                // force pagination, and pages to be fetched on demand as the ResultSet is iterated on
                .setFetchSize(numberOfRecords / numberOfPages));

        results.doOnNext(rows -> rows.forEach(row ->
                assertThat(row.getString(0)).startsWith("job-"))
        ).toCompletable().await(1, TimeUnit.MINUTES);
    }
}