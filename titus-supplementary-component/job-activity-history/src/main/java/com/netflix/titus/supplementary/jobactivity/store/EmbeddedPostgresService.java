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

package com.netflix.titus.supplementary.jobactivity.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.inject.Inject;
import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class EmbeddedPostgresService {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedPostgresService.class);

    private static final String DB_NAME = "integration";

    private static final String POSTGRES_DB_NAME = "postgres", POSTGRES_USER = "postgres", POSTGRES_PW = "postgres";

    private final EmbeddedPostgres embeddedPostgres;
    private final DataSource dataSource;
    private final Connection connection;

    @Inject
    public EmbeddedPostgresService(JooqConfigurationBean configuration) {
        if (configuration.isInMemoryDb()) {
            try {
                this.embeddedPostgres = EmbeddedPostgres.start();

                DataSource defaultDataSource = embeddedPostgres.getDatabase(POSTGRES_USER, POSTGRES_DB_NAME);
                connection = defaultDataSource.getConnection(POSTGRES_USER, POSTGRES_PW);
                PreparedStatement statement = connection.prepareStatement("CREATE DATABASE integration");
                // ignore result
                statement.execute();
                statement.close();
                this.dataSource = this.embeddedPostgres.getDatabase(POSTGRES_USER, DB_NAME);
            } catch (Exception error) {
                logger.error("Failed to start an instance of EmbeddedPostgres database", error);
                throw new IllegalStateException(error);
            }
        } else {
            this.embeddedPostgres = null;
            this.dataSource = null;
            this.connection = null;
        }
    }


    public DataSource getDataSource() {
        Preconditions.checkNotNull(dataSource, "Embedded Postgres mode not enabled");
        return dataSource;
    }

}
