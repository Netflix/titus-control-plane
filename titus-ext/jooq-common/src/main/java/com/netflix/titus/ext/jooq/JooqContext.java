/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.jooq;

import javax.sql.DataSource;

import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;

public class JooqContext {

    private static final SQLDialect DEFAULT_DIALECT = SQLDialect.POSTGRES;

    private final JooqConfiguration jooqConfiguration;
    private final DataSource dataSource;
    private final EmbeddedPostgresService embeddedPostgresService;
    private final DefaultDSLContext dslContext;

    public JooqContext(JooqConfiguration jooqConfiguration,
                       DataSource dataSource,
                       EmbeddedPostgresService embeddedPostgresService) {
        this.jooqConfiguration = jooqConfiguration;
        this.dataSource = dataSource;
        this.embeddedPostgresService = embeddedPostgresService;
        this.dslContext = new DefaultDSLContext(dataSource, DEFAULT_DIALECT);
    }

    public JooqConfiguration getJooqConfiguration() {
        return jooqConfiguration;
    }

    public EmbeddedPostgresService getEmbeddedPostgresService() {
        return embeddedPostgresService;
    }

    public SQLDialect getDialect() {
        return DEFAULT_DIALECT;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DefaultDSLContext getDslContext() {
        return dslContext;
    }
}
